/*************************************************************************
 *
 * Copyright 2018 Ideas2IT Technology Services Private Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ***********************************************************************/

package goclient

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	pb "github.com/Suresh-Nakkeran/ideacrawler/protofiles"
	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type PageHTML = pb.PageHTML
type JobID = pb.JobID

type Worker struct {
	Conn   *grpc.ClientConn
	Client pb.IdeaCrawlerClient
}

type JobSpec struct {
	dopt *pb.DomainOpt

	implPageChan        chan *pb.PageHTML
	useAnalyzedURLChan  bool
	implAnalyzedURLChan chan *pb.UrlList
	callbackSeedURL     bool

	cleanUpFunc func()
}

type CrawlJob struct {
	spec *JobSpec

	startedChan    chan struct{} // if this is closed, job has started.
	stoppedChan    chan struct{} // if this is closed, job has stopped.
	addPagesClient pb.IdeaCrawler_AddPagesClient
	jobClient      pb.IdeaCrawler_AddDomainAndListenClient
	id             *pb.JobID

	PageChan        <-chan *pb.PageHTML
	AnalyzedURLChan <-chan *pb.UrlList
	Worker          *Worker
}

type KVMap = map[string]string

const (
	PageReqType_BUILTINJS = pb.PageReqType_BUILTINJS
	PageReqType_JSCRIPT   = pb.PageReqType_JSCRIPT
)

func NewWorker(socket string, tcpconn net.Conn) (*Worker, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	if tcpconn != nil {
		opts = append(opts, grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
			return tcpconn, nil
		}))
		opts = append(opts, grpc.WithDisableRetry())
	}
	conn, err := grpc.Dial(socket, opts...)
	if err != nil {
		return nil, err
	}
	client := pb.NewIdeaCrawlerClient(conn)
	return &Worker{conn, client}, nil
}

func (w *Worker) Close() {
	w.Conn.Close()
}

func (w *Worker) GetWorkerID() (string, error) {
	wid, err := w.Client.GetWorkerID(context.Background(), &empty.Empty{})
	if err != nil {
		return "", err
	}
	return wid.ID, nil
}

func NewJobSpec(opts ...Option) *JobSpec {
	dopt := &pb.DomainOpt{
		MinDelay:              5,
		Depth:                 -1,
		DomLoadTime:           5,
		Useragent:             "Fetchbot",
		MaxConcurrentRequests: 5,
	}
	spec := &JobSpec{
		dopt: dopt,
	}
	for _, opt := range opts {
		opt(spec)
	}
	return spec
}

func NewCrawlJob(w *Worker, spec *JobSpec) (*CrawlJob, error) {
	jobClient, err := w.Client.AddDomainAndListen(context.Background(), spec.dopt, grpc.MaxCallRecvMsgSize((2*1024*1024*1024)-1))
	if err != nil {
		log.Println("Box is possibly down. AddDomainAndListen failed:", err)
		return nil, err
	}
	jobIDPage, err := jobClient.Recv()
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	if spec.implPageChan == nil {
		spec.implPageChan = NewPageChan()
	}

	if spec.useAnalyzedURLChan == true && spec.implAnalyzedURLChan == nil {
		spec.implAnalyzedURLChan = NewAnalyzedURLChan()
	}

	var cj = &CrawlJob{
		Worker:          w,
		spec:            spec,
		startedChan:     make(chan struct{}),
		stoppedChan:     make(chan struct{}),
		id:              jobIDPage.JobID,
		jobClient:       jobClient,
		PageChan:        spec.implPageChan,
		AnalyzedURLChan: spec.implAnalyzedURLChan,
	}

	go cj.run()
	time.Sleep(2 * time.Second)

	return cj, nil
}

func (cj *CrawlJob) initAnalyzedURL() {
	if cj.spec.useAnalyzedURLChan == false {
		return
	}
	if cj.id == nil {
		log.Println("No job subscription. SetAnalyzedURLs failed.")
		return
	}
	urlstream, err := cj.Worker.Client.GetAnalyzedURLs(context.Background(), cj.id, grpc.MaxCallRecvMsgSize((2*1024*1024*1024)-1))
	if err != nil {
		log.Println("Box is possibly down. SetAnalyzedURLs failed:", err)
		return
	}
	go cj.listenAnalyzedURLs(urlstream)
}

func (cj *CrawlJob) listenAnalyzedURLs(urlstream pb.IdeaCrawler_GetAnalyzedURLsClient) {
	for {
		urlList, err := urlstream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
			break
		}
		cj.spec.implAnalyzedURLChan <- urlList
	}
}

func (cj *CrawlJob) AddPage(url, metaStr string) error {
	if cj.IsAlive() == false {
		errorMsg := "AddPage function can't be called when crawl job is not running. Please start crawl job first then call Addpage."
		log.Println(errorMsg)
		return errors.New(errorMsg)
	}
	if cj.addPagesClient == nil {
		var err error
		cj.addPagesClient, err = cj.Worker.Client.AddPages(context.Background())
		if err != nil {
			cj.addPagesClient = nil
			return err
		}
	}
	return cj.addPagesClient.Send(&pb.PageRequest{
		JobID:   cj.id,
		Reqtype: pb.PageReqType_GET,
		Url:     url,
		MetaStr: metaStr,
	})
}

func (cj *CrawlJob) AddJS(typ pb.PageReqType, url, js, metaStr string) error {
	if cj.IsAlive() == false {
		errorMsg := "AddJS function can't be called when crawl job is not running. Please start crawl job first then call AddJS."
		log.Println(errorMsg)
		return errors.New(errorMsg)
	}
	if cj.addPagesClient == nil {
		var err error
		cj.addPagesClient, err = cj.Worker.Client.AddPages(context.Background())
		if err != nil {
			cj.addPagesClient = nil
			return err
		}
	}
	return cj.addPagesClient.Send(&pb.PageRequest{
		JobID:   cj.id,
		Reqtype: typ,
		Url:     url,
		Js:      js,
		MetaStr: metaStr,
	})
}

func (cj *CrawlJob) IsAlive() bool {
	select {
	case <-cj.startedChan:
	default: // startedChan is still open, so job has not started
		return false
	}
	select {
	case _, ok := <-cj.stoppedChan:
		if ok == false {
			return false
		}
	default: // stoppedChan is still open, so job is running
		return true
	}
	return false // never reached
}

func (cj *CrawlJob) Stop() {
	if cj.IsAlive() {
		cj.Worker.Client.CancelJob(context.Background(), cj.id)
	}
}

func (cj *CrawlJob) run() {
	close(cj.startedChan)
	defer func() {
		close(cj.stoppedChan)
		if cj.spec.cleanUpFunc != nil {
			cj.spec.cleanUpFunc()
		}
	}()

	cj.initAnalyzedURL()
	phChan := make(chan *pb.PageHTML, 1000)
	defer close(phChan)

	go func() {
		time.Sleep(3 * time.Second) // This is to make sure callbacks don't start until Start() function exits.  Start sleep for 2 seconds.
		for ph := range phChan {
			cj.spec.implPageChan <- ph
		}
	}()

	for {
		page, err := cj.jobClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
			break
		}
		phChan <- page
	}
}

func NewPageChan() chan *PageHTML {
	return make(chan *PageHTML, 100)
}

func NewAnalyzedURLChan() chan *pb.UrlList {
	return make(chan *pb.UrlList, 100)
}
