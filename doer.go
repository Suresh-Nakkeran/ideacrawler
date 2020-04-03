package main

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Suresh-Nakkeran/fetchbot"
	"github.com/Suresh-Nakkeran/ideacrawler/prefetchurl"
	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
)

// Doer is an implementation of the fetchbot.Doer interface,
// used for pre-processing outgoing http requests and rate control.
type Doer struct {
	doer fetchbot.Doer
	job  *job
	sema *semaphore.Weighted
	s    *ideaCrawlerWorker
}

// Do does the following:
//   - enforce MaxConcurrentRequests, waits until there are slots.
//   - if using OpenVPN, ensures virtual network interface exists.
//   - provides randomized delay in maxDelay - minDelay range.
//   - when enabled, prefetches css, js, etc. to emulate browser.
func (gc *Doer) Do(req *http.Request) (*http.Response, error) {
	opts := gc.job.opts
	if gc.sema == nil {
		gc.sema = semaphore.NewWeighted(int64(opts.MaxConcurrentRequests))
	}
	gc.sema.Acquire(context.Background(), 1)
	defer gc.sema.Release(1)
	if opts.NetworkIface != "" {
		ifs, err := net.Interfaces()
		if err != nil {
			gc.job.log.Printf("ERR - Unable to get list of network interfaces. Error - %v\n", err)
			return nil, err
		}
		ifmatch := false
		for _, iface := range ifs {
			if iface.Name == opts.NetworkIface {
				ifmatch = true
				break
			}
		}
		if ifmatch == false {
			emsg := "Interface '" + opts.NetworkIface + "' not active."
			gc.job.cancelChan <- struct{}{}
			gc.job.log.Printf("ERR - %v\n", emsg)
			return nil, errors.New(emsg)
		}
	}
	// randomized delay
	afterTime := opts.MinDelay
	if afterTime < 1 {
		afterTime = 1
	}
	if opts.MaxDelay > afterTime {
		afterTime = int32(<-gc.job.randChan)
		gc.job.log.Printf("Next delay - %v\n", time.Duration(afterTime)*time.Second)
	}
	after := time.After(time.Duration(afterTime) * time.Second)
	resp, err := gc.doer.Do(req)
	if err != nil {
		return nil, err
	}
	// process prefetch urls
	if opts.Chrome == false && opts.Prefetch == true {
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("ERR - Unable to read url response content. Error - %v\n", err)
			return nil, err
		}
		err = processPrefetchURLs(gc, respBody, resp.Request.URL.String())
		if err != nil {
			return nil, err
		}
		resp.Body = ioutil.NopCloser(bytes.NewBuffer(respBody))
	}
	<-after
	return resp, nil
}

func randomGenerator(min int, max int, randChan chan<- int) {
	ii := 0
	jj := 5
	genRand := func(min int, max int) int {
		v := 0
		for v < min {
			v = int((rand.NormFloat64()+1.0)*float64(max-min)/2.0 + float64(min))
		}
		return v
	}
	for ; ; ii++ {
		if ii >= jj {
			jj = genRand(5, 20)
			ii = 0
			randChan <- genRand(max, max*3)
			continue
		}
		randChan <- genRand(min, max)
	}
}

func execPrefetchReq(doer *Doer, prefetchReq *http.Request) {
	res, _ := doer.doer.Do(prefetchReq)
	io.Copy(ioutil.Discard, res.Body)
	res.Body.Close()
}

func processPrefetchCSSURL(doer *Doer, prefetchCSSReq *http.Request, prefetchCSSURL string) error {
	var err error
	prefetchCSSRes, err := doer.doer.Do(prefetchCSSReq)
	if err != nil {
		log.Printf("ERR - Unable to process prefetch css resource url. Error - %v\n", err)
		return err
	}
	prefetchCSSResBody, err := ioutil.ReadAll(prefetchCSSRes.Body)
	if err != nil {
		log.Printf("ERR - Unable to read css url response content. Error - %v\n", err)
		return err
	}
	prefetchLinks, err := prefetchurl.GetPrefetchURLs(prefetchCSSResBody, prefetchCSSURL)
	if err != nil {
		log.Printf("ERR - While trying to retreive resources urls. Error - %v\n", err)
		return err
	}
	for _, prefetchLink := range prefetchLinks {
		prefetchLinkReq, err := http.NewRequest("GET", prefetchLink, nil)
		if err != nil {
			log.Printf("ERR - Unable to create new request for prefetch urls. Error - %v\n", err)
			return err
		}
		prefetchLinkReq.Header.Set("User-Agent", doer.job.opts.Useragent)
		go execPrefetchReq(doer, prefetchLinkReq)
	}
	return err
}

func processPrefetchURLs(doer *Doer, respBody []byte, reqURL string) error {
	var err error
	prefetchLinks, err := prefetchurl.GetPrefetchURLs(respBody, reqURL)
	if err != nil {
		log.Printf("ERR - While trying to retreive resources urls. Error - %v\n", err)
		return err
	}
	for _, prefetchLink := range prefetchLinks {
		prefetchLinkReq, err := http.NewRequest("GET", prefetchLink, nil)
		if err != nil {
			log.Printf("ERR - Unable to create new request for prefetch urls. Error - %v\n", err)
			return err
		}
		prefetchLinkReq.Header.Set("User-Agent", doer.job.opts.Useragent)
		if strings.HasSuffix(prefetchLink, ".css") {
			go processPrefetchCSSURL(doer, prefetchLinkReq, prefetchLink)
		} else {
			go execPrefetchReq(doer, prefetchLinkReq)
		}
	}
	return err
}
