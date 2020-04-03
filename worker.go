package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/Suresh-Nakkeran/ideacrawler/chromeclient"
	pb "github.com/Suresh-Nakkeran/ideacrawler/protofiles"
	sc "github.com/Suresh-Nakkeran/ideacrawler/statuscodes"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	"github.com/hashicorp/yamux"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type subscriber struct {
	doneSeqnum           int32
	reqChan              chan pb.PageRequest
	sendChan             chan pb.PageHTML
	stopChan             chan bool
	analyzedURLChan      chan pb.UrlList
	stopAnalyzedURLChan  chan bool
	analyzedURLConnected bool
	connected            bool
}

type ideaCrawlerWorker struct {
	workerID   string
	mode       int
	jobs       map[string]*job
	newJobChan chan newJob
	newSubChan chan newSub
	ccl        *chromeclient.ChromeClient

	wm  *workerManager
	cwm *chromeWorkerManager
}

type newJobStatus struct {
	job *job
	err error
}

type newJob struct {
	opts      *pb.DomainOpt
	retChan   chan<- newJobStatus
	subscribe bool
}

type newSub struct {
	sub     pb.JobID
	retChan chan<- newJobStatus
}

type crawlCommand struct {
	method     string
	url        *url.URL
	noCallback bool
	metaStr    string
	urlDepth   int32
	anchorText string
}

func newCrawlCommand(method, urlstr, metaStr string, urlDepth int32) (crawlCommand, error) {
	parsed, err := url.Parse(urlstr)
	return crawlCommand{
		method:   method,
		url:      parsed,
		metaStr:  metaStr,
		urlDepth: urlDepth,
	}, err
}

func (c crawlCommand) URL() *url.URL {
	return c.url
}

func (c crawlCommand) Method() string {
	return c.method
}

func (c crawlCommand) MetaStr() string {
	return c.metaStr
}

func (c crawlCommand) URLDepth() int32 {
	return c.urlDepth
}

func domainNameFromURL(_url string) (string, error) { //returns domain name and error if any
	u, err := url.Parse(_url)
	if err != nil {
		return "", err
	}
	return u.Hostname(), nil
}

// addNewJob does validation on incoming jobs from clients, and adds
// them to the server's job pool.
func (s *ideaCrawlerWorker) addNewJob(nj newJob) {
	log.Println("Received new job", nj.opts.SeedUrl)
	domainname, err := domainNameFromURL(nj.opts.SeedUrl)
	var jobStatusFailureMessage = func(err error) newJobStatus {
		return newJobStatus{
			job: nil,
			err: err,
		}
	}
	if err != nil {
		nj.retChan <- jobStatusFailureMessage(err)
		return
	}

	sub := pb.JobID{
		ID: uuid.New().String(),
	}

	subr := &subscriber{}
	if nj.subscribe == true {
		subr = &subscriber{
			doneSeqnum:           0,
			reqChan:              make(chan pb.PageRequest, 1000),
			sendChan:             make(chan pb.PageHTML, 1000),
			stopChan:             make(chan bool, 3),
			analyzedURLChan:      nil,
			stopAnalyzedURLChan:  nil,
			analyzedURLConnected: false,
			connected:            true,
		}
	}
	var callbackURLRegexp, followURLRegexp, callbackAnchorTextRegexp *regexp.Regexp
	if len(nj.opts.CallbackUrlRegexp) > 0 {
		callbackURLRegexp, err = regexp.Compile(nj.opts.CallbackUrlRegexp)
		if err != nil {
			nj.retChan <- jobStatusFailureMessage(fmt.Errorf("CallbackUrlRegexp doesn't compile - %s - %s'", nj.opts.CallbackUrlRegexp, err))
			return
		}
	}
	if len(nj.opts.FollowUrlRegexp) > 0 {
		followURLRegexp, err = regexp.Compile(nj.opts.FollowUrlRegexp)
		if err != nil {
			nj.retChan <- jobStatusFailureMessage(fmt.Errorf("FollowURLRegexp doesn't compile - %s - %s", nj.opts.FollowUrlRegexp, err))
			return
		}
	}
	if len(nj.opts.CallbackAnchorTextRegexp) > 0 {
		callbackAnchorTextRegexp, err = regexp.Compile(nj.opts.CallbackAnchorTextRegexp)
		if err != nil {
			nj.retChan <- jobStatusFailureMessage(fmt.Errorf("AnchorTextRegexp doesn't compile - %s - %s", nj.opts.CallbackAnchorTextRegexp, err))
			return
		}
	}
	randChan := make(chan int, 5)
	j := &job{
		domainname:               domainname,
		opts:                     nj.opts,
		id:                       sub,
		seqnum:                   0,
		callbackURLRegexp:        callbackURLRegexp,
		followURLRegexp:          followURLRegexp,
		callbackAnchorTextRegexp: callbackAnchorTextRegexp,
		subscriber:               subr,
		mu:                       sync.Mutex{},
		duplicates:               map[string]bool{},
		cancelChan:               make(chan struct{}),
		doneChan:                 make(chan struct{}),
		randChan:                 randChan,
		log:                      nil,
	}
	s.jobs[sub.ID] = j
	go randomGenerator(int(nj.opts.MinDelay), int(nj.opts.MaxDelay), randChan)
	go s.RunJob(sub.ID, j)
	nj.retChan <- newJobStatus{
		job: j,
		err: nil,
	}
}

func (s *ideaCrawlerWorker) jobManager(newJobChan <-chan newJob, newSubChan <-chan newSub) {
	for {
		select {
		case nj := <-newJobChan:
			s.addNewJob(nj)
		case ns := <-newSubChan:
			job := s.jobs[ns.sub.ID]
			if job == nil {
				ns.retChan <- newJobStatus{
					job: nil,
					err: errors.New("Unable to find subcode - " + ns.sub.ID),
				}
				continue
			}
			ns.retChan <- newJobStatus{
				job: job,
				err: nil,
			}
		default:
		}
		for subcode, job := range s.jobs {
			if job.done() {
				delete(s.jobs, subcode)
				continue
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (s *ideaCrawlerWorker) AddPages(stream pb.IdeaCrawler_AddPagesServer) error {
	pgreq1, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	if pgreq1 == nil {
		emsg := "Received nil pagereq in AddPages.  Exiting AddPages"
		log.Println(emsg)
		return errors.New(emsg)
	}
	if pgreq1.JobID == nil {
		emsg := fmt.Sprintf("Received pagereq with nil sub object. Exiting AddPages.  PageReq - %v", pgreq1)
		log.Println(emsg)
		return errors.New(emsg)
	}
	retChan := make(chan newJobStatus)
	s.newSubChan <- newSub{*pgreq1.JobID, retChan}
	njs := <-retChan
	if njs.err != nil {
		return njs.err
	}
	job := njs.job
	reqChan := job.subscriber.reqChan
	reqChan <- *pgreq1
	log.Printf("Adding new page for job '%v': %v", pgreq1.JobID.ID, pgreq1.Url)
	for {
		pgreq, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if pgreq == nil {
			emsg := "Received nil pagereq in AddPages.  Exiting AddPages"
			log.Println(emsg)
			return errors.New(emsg)
		}
		select {
		case <-job.doneChan:
			return nil
		default:
			time.Sleep(10 * time.Millisecond)
		}
		reqChan <- *pgreq
		log.Printf("Adding new page for job '%v': %v", pgreq.JobID.ID, pgreq.Url)
	}
}

func (s *ideaCrawlerWorker) CancelJob(ctx context.Context, sub *pb.JobID) (*pb.Status, error) {
	if sub == nil {
		emsg := "Received nil subscription in CancelJob.  Not canceling anything."
		log.Println(emsg)
		return &pb.Status{Success: false, Error: emsg}, errors.New(emsg)
	}
	log.Println("Cancel request received for job:", sub.ID)
	retChan := make(chan newJobStatus)
	s.newSubChan <- newSub{*sub, retChan}
	njs := <-retChan
	if njs.err != nil {
		log.Println("ERR - Cancel failed -", njs.err.Error())
		return &pb.Status{Success: false, Error: njs.err.Error()}, njs.err
	}
	njs.job.cancelChan <- struct{}{}
	return &pb.Status{Success: true, Error: ""}, nil
}

func (s *ideaCrawlerWorker) GetAnalyzedURLs(sub *pb.JobID, ostream pb.IdeaCrawler_GetAnalyzedURLsServer) error {
	if sub == nil {
		emsg := "Received nil subscription in GetAnalyzedURLs.  Not requesting analyzed urls."
		log.Println(emsg)
		return errors.New(emsg)
	}
	log.Println("Analyzed urls request received for job:", sub.ID)
	retChan := make(chan newJobStatus)
	s.newSubChan <- newSub{*sub, retChan}
	njs := <-retChan
	if njs.err != nil {
		log.Println("ERR - Get analyzed urls request failed -", njs.err.Error())
		return njs.err
	}
	job := njs.job
	analyzedURLChan := make(chan pb.UrlList, 100)
	stopAnalyzedURLChan := make(chan bool, 3)

	job.subscriber.analyzedURLConnected = true
	job.subscriber.analyzedURLChan = analyzedURLChan
	job.subscriber.stopAnalyzedURLChan = stopAnalyzedURLChan
	log.Println("Analyzed urls request registered")
	for urlList := range job.subscriber.analyzedURLChan {
		err := ostream.Send(&urlList)
		if err != nil {
			log.Printf("Failed to send analyzed urls to client. No longer trying - %v. Error - %v\n", job.id.ID, err)
			job.subscriber.stopAnalyzedURLChan <- true
			return err
		}
	}
	return nil
}

func (s *ideaCrawlerWorker) AddDomainAndListen(opts *pb.DomainOpt, ostream pb.IdeaCrawler_AddDomainAndListenServer) error {
	retChan := make(chan newJobStatus)
	s.newJobChan <- newJob{opts, retChan, true}
	njs := <-retChan
	if njs.err != nil {
		return njs.err
	}
	job := njs.job
	if job.subscriber.connected == false {
		return errors.New("Subscriber object not created")
	}
	log.Println("Sending subscription object to client:", job.id.ID)
	// send an empty pagehtml with just the subscription object,  as soon as job starts.
	err := ostream.Send(&pb.PageHTML{
		Success:        true,
		Error:          "subscription.object",
		JobID:          &job.id,
		Url:            "",
		Httpstatuscode: sc.Subscription,
		Content:        []byte{},
	})
	if err != nil {
		log.Printf("Failed to send sub object to client. Cancelling job - %v. Error - %v\n", job.id.ID, err)
		job.subscriber.stopChan <- true
		return err
	}

	for pagehtml := range job.subscriber.sendChan {
		err := ostream.Send(&pagehtml)
		if err != nil {
			log.Printf("Failed to send page back to client. No longer trying - %v. Error - %v\n", job.id.ID, err)
			job.subscriber.stopChan <- true
			return err
		}
	}
	return nil
}

func (s *ideaCrawlerWorker) GetWorkerID(context.Context, *empty.Empty) (*pb.WorkerID, error) {
	return &pb.WorkerID{
		ID: s.workerID,
	}, nil
}

func newClusterWorkerListener() net.Listener {
	conn, err := net.Dial("tcp", cliParams.Servers)
	if err != nil {
		log.Fatalf("Unable to connect to servers: %v", err)
	}
	session, err := yamux.Server(conn, nil)
	if err != nil {
		log.Fatalf("Unable to connect to servers: %v", err)
	}
	return session
}

func newStandaloneListener() net.Listener {
	lis, err := net.Listen("tcp", cliParams.ClientListenAddress)
	if err != nil {
		log.Printf("failed to listen: %v", err)
		os.Exit(1)
	}
	log.Println("Listening on", cliParams.ClientListenAddress)
	return lis
}

func newServer(mode int, wm *workerManager) *ideaCrawlerWorker {
	s := new(ideaCrawlerWorker)
	s.jobs = make(map[string]*job)
	s.newJobChan = make(chan newJob)
	s.newSubChan = make(chan newSub)
	s.workerID = uuid.New().String()
	s.mode = mode
	s.wm = wm
	s.cwm = (*chromeWorkerManager)(wm)
	return s
}

func startCrawlerWorker(mode int, srv *ideaCrawlerWorker) {
	var lis net.Listener
	if mode == modeStandalone {
		lis = newStandaloneListener()
	} else if mode == modeWorker {
		lis = newClusterWorkerListener()
	}
	defer log.Println("Exiting crawler. Bye")
	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	pb.RegisterIdeaCrawlerServer(grpcServer, srv)
	go srv.jobManager(srv.newJobChan, srv.newSubChan)
	grpcServer.Serve(lis)
}
