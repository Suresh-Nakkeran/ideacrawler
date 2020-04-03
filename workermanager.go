package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"

	gc "github.com/Suresh-Nakkeran/ideacrawler/goclient"
	"github.com/google/uuid"
	"github.com/hashicorp/yamux"
)

type worker struct {
	id   string
	w    *gc.Worker
	job  *gc.CrawlJob // raw job
	cJob *gc.CrawlJob // chrome job
}

type workerManager struct {
	workers      *workerQueue
	addChan      chan *worker // add new worker
	nextReqChan  chan struct{}
	nextChan     chan *worker // get next worker
	pageChan     chan *gc.PageHTML
	waitRegnChan chan *waitRegn
}

func newWorkerManager() *workerManager {
	wm := &workerManager{
		workers:     new(workerQueue),
		addChan:     make(chan *worker, 10),
		nextReqChan: make(chan struct{}, 10),
		nextChan:    make(chan *worker, 10),

		pageChan:     gc.NewPageChan(),
		waitRegnChan: make(chan *waitRegn),
	}
	return wm
}

func (wm *workerManager) addWorker(w *worker) {
	wm.addChan <- w
}

func (wm *workerManager) nextWorker() *worker {
	wm.nextReqChan <- struct{}{}
	return <-wm.nextChan
}

func (wm *workerManager) run() {
	for {
		select {
		case w := <-wm.addChan:
			// TODO: check worker already exists
			wm.workers.push(w)
		case <-wm.nextReqChan:
			wm.nextChan <- wm.workers.next()
		}
	}
}

type waitRegn struct {
	id       string
	respChan chan *gc.PageHTML
}

func (wm *workerManager) respHandler() {
	waitingList := map[string]chan *gc.PageHTML{}
	for {
		select {
		case newWait := <-wm.waitRegnChan:
			waitingList[newWait.id] = newWait.respChan
		case resp := <-wm.pageChan:
			if resp == nil {
				log.Println("Received empty response from pageChan.")
				continue
			}
			if resp.MetaStr == "" {
				log.Println("Received resp with empty MetaStr")
				continue
			}
			respChan := waitingList[resp.MetaStr]
			respChan <- resp
			//			delete(waitingList, resp.MetaStr)
		}
	}
}

func (wm *workerManager) Do(req *http.Request) (*http.Response, error) {
	w := wm.nextWorker()
	if w == nil {
		return nil, errors.New("no active workers")
	}
	wRespID := uuid.New().String()
	err := w.job.AddPage(req.URL.String(), wRespID)
	if err != nil {
		return nil, err
	}
	respChan := make(chan *gc.PageHTML)
	wm.waitRegnChan <- &waitRegn{
		id:       wRespID,
		respChan: respChan,
	}
	resp := <-respChan
	hresp := &http.Response{
		StatusCode: int(resp.Httpstatuscode),
		Status:     http.StatusText(int(resp.Httpstatuscode)),
		Body:       ioutil.NopCloser(bytes.NewBuffer(resp.Content)),
		Request:    req,
		Header:     make(http.Header),
	}
	hresp.Header.Set("Content-Type", "text/html")
	return hresp, nil
}

type chromeWorkerManager workerManager

func (cwm *chromeWorkerManager) Do(req *http.Request) (*http.Response, error) {
	wm := (*workerManager)(cwm)
	w := wm.nextWorker()
	if w == nil {
		return nil, errors.New("no active workers")
	}
	wRespID := uuid.New().String()
	// TODO: need a way to set cookies from req
	err := w.cJob.AddPage(req.URL.String(), wRespID)
	if err != nil {
		return nil, err
	}
	respChan := make(chan *gc.PageHTML)
	wm.waitRegnChan <- &waitRegn{
		id:       wRespID,
		respChan: respChan,
	}
	resp := <-respChan
	hresp := &http.Response{
		StatusCode: int(resp.Httpstatuscode),
		Body:       ioutil.NopCloser(bytes.NewBuffer(resp.Content)),
		Request:    req,
		Header:     make(http.Header),
	}
	hresp.Header.Set("Content-Type", "text/html")
	return hresp, nil
}

func (wm *workerManager) start() {
	go wm.run()
	go wm.respHandler()
	go wm.startClusterListener()
}

func (wm *workerManager) startClusterListener() {
	listener, err := net.Listen("tcp", cliParams.ClusterListenAddress)
	if err != nil {
		log.Printf("failed to listen: %#v", err)
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Println("Listening on", cliParams.ClusterListenAddress)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("failed to accept: %#v", err)
			continue
		}
		go wm.createNewClient(conn)
	}
}

func (wm *workerManager) createNewClient(conn net.Conn) {
	session, err := yamux.Client(conn, nil)
	if err != nil {
		log.Println("failed to create yamux client:", err)
		return
	}
	// Open a new stream
	yconn, err := session.Open()
	if err != nil {
		log.Println("failed to open yamux conn:", err)
		return
	}
	w, err := gc.NewWorker("", yconn)
	if err != nil {
		log.Println("failed to create gc.Worker:", err)
		return
	}
	j := newWorkerJob(w, false, "", wm.pageChan)
	if j == nil {
		return
	}
	cj := newWorkerJob(w, true, "/usr/bin/chromium", wm.pageChan)
	if j == nil {
		return
	}

	id, err := w.GetWorkerID()
	if err != nil {
		log.Println("failed to get workerID:", err)
		return
	}
	// TODO: grpc getWorkerID
	wm.addWorker(&worker{
		id:   id,
		w:    w,
		job:  j,
		cJob: cj,
	})
}

func newWorkerJob(w *gc.Worker, chrome bool, chromeBinary string, pageChan chan *gc.PageHTML) *gc.CrawlJob {
	opts := []gc.Option{
		gc.NoFollow(),
		gc.Impolite(),
		gc.PageChan(pageChan),
		gc.MaxIdleTime(365 * 24 * 3600),
		gc.Chrome(chrome, chromeBinary),
	}

	z, err := gc.NewCrawlJob(w, gc.NewJobSpec(opts...))
	if err != nil {
		log.Println("failed to create CrawlJob:", err)
		return nil
	}
	return z

}
