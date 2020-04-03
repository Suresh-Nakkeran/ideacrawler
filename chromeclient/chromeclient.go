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

package chromeclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mafredri/cdp"
	"github.com/mafredri/cdp/devtool"
	"github.com/mafredri/cdp/protocol/dom"
	"github.com/mafredri/cdp/protocol/network"
	"github.com/mafredri/cdp/protocol/page"
	"github.com/mafredri/cdp/protocol/runtime"
	"github.com/mafredri/cdp/protocol/target"
	"github.com/mafredri/cdp/rpcc"
	"github.com/mafredri/cdp/session"
	"github.com/phayes/freeport"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type ChromeClient struct {
	cpath         string // path to browser binary
	userAgent     string
	scrollCount   int32
	disableImages bool
	devt          *devtool.DevTools
	pageTgt       *devtool.Target
	conn          *rpcc.Conn
	c             *cdp.Client
	mgr           *session.Manager
	ctx           context.Context
	cancel        context.CancelFunc
	chromeCmd     *exec.Cmd
	domLoadTime   time.Duration
	rspRcvd       network.ResponseReceivedClient
	started       bool
	mu            sync.Mutex
	up            bool
	sema          *semaphore.Weighted
}

type ChromeDoer struct {
	domLoadTime time.Duration
	cc          *ChromeClient
	ctx         context.Context
	cancel      context.CancelFunc
}

func NewChromeClient(path string, userAgent string, disableImages bool, scrollCount int32) *ChromeClient {
	ctx, cancel := context.WithCancel(context.Background())

	return &ChromeClient{
		cpath:         path,
		userAgent:     userAgent,
		disableImages: disableImages,
		scrollCount:   scrollCount,
		devt:          nil,
		chromeCmd:     nil,
		ctx:           ctx,
		cancel:        cancel,
		domLoadTime:   5 * time.Second, // default 5 secs for page to load in browser before we get dump.
		sema:          semaphore.NewWeighted(1),
	}
}

func (cc *ChromeClient) CheckChromeProcess() error {
	if cc.Up() == false {
		err := cc.Start()
		if err != nil {
			log.Println("Unable to start chrome:", err)
			return err
		}
	}
	return nil
}

func (cc *ChromeClient) NewChromeDoer() *ChromeDoer {
	ctx, cancel := context.WithCancel(context.Background())

	return &ChromeDoer{
		domLoadTime: cc.domLoadTime,
		cc:          cc,
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (cc *ChromeClient) SetDomLoadTime(secs int32) {
	cc.domLoadTime = time.Duration(secs) * time.Second
}

func (cd *ChromeDoer) SetDomLoadTime(secs int32) {
	cd.domLoadTime = time.Duration(secs) * time.Second
}

func (cc *ChromeClient) Stop() {
	cc.conn.Close()
	cc.cancel()
	err := cc.chromeCmd.Process.Kill()
	if err != nil {
		log.Println(err)
	}
}

func (cc *ChromeClient) Up() bool {
	if cc == nil {
		return false
	}
	return cc.up
}

func (cc *ChromeClient) Start() error {
	var err error
	port, err := freeport.GetFreePort()
	if err != nil {
		log.Println(err)
		return err
	}

	// TODO: add support for starting chrome from a container.
	portStr := strconv.Itoa(port)
	cmd_args := []string{"--headless", "--no-sandbox", "--disable-popup-blocking", "--ignore-certificate-errors", "--allow-running-insecure-content", "--disable-extensions", "--disable-gpu", "--remote-debugging-port=" + portStr}
	if cc.userAgent != "" {
		cmd_args = append(cmd_args, []string{"--user-agent=" + cc.userAgent, "--window-size=1920,1080"}...)
	}
	if cc.disableImages == true {
		cmd_args = append(cmd_args, "'chrome.prefs': {'profile.managed_default_content_settings.images': 2}")
	}
	cc.chromeCmd = exec.Command(cc.cpath, cmd_args...)
	err = cc.chromeCmd.Start()
	if err != nil {
		log.Println("Unable to start chrome browser in path '" + cc.cpath + "'. Error - " + err.Error())
		return err
	}

	go func() {
		cc.up = true
		cc.chromeCmd.Wait()
		cc.up = false
	}()

	time.Sleep(3 * time.Second) // TODO: make customizable. give a few seconds for browser to start.

	cc.devt = devtool.New("http://localhost:" + portStr)
	cc.pageTgt, err = cc.devt.Get(cc.ctx, devtool.Page)
	if err != nil {
		log.Println(err)
		return err
	}

	cc.conn, err = rpcc.DialContext(cc.ctx, cc.pageTgt.WebSocketDebuggerURL)
	if err != nil {
		log.Println(err)
		return err
	}

	// Create a new CDP Client that uses conn.
	cc.c = cdp.NewClient(cc.conn)

	if err = runBatch(
		// Enable all the domain events that we're interested in.
		func() error { return cc.c.DOM.Enable(cc.ctx) },
		func() error { return cc.c.Network.Enable(cc.ctx, nil) },
		func() error { return cc.c.Page.Enable(cc.ctx) },
		func() error { return cc.c.Runtime.Enable(cc.ctx) },
	); err != nil {
		log.Println(err)
		return err
	}

	cc.mgr, err = session.NewManager(cc.c)
	if err != nil {
		log.Println(err)
		return err
	}
	return err
}

// "http://" types,  or "crawljs-builtinjs://<hostname>/<js>?<url> or
//                      "crawljs-jscript://<hostname>/<builtin OP name>"
func (cd *ChromeDoer) Do(req *http.Request) (resp *http.Response, err error) {
	if strings.HasPrefix(req.URL.Scheme, "crawljs") {
		return cd.doJS(req)
	}
	r, _, e := cd.doNavigate(req, false)
	return r, e
}

type chromePage struct {
	targetID target.ID
	conn     *rpcc.Conn
	client   *cdp.Client
	rspRcvd  network.ResponseReceivedClient
}

func (cd *ChromeDoer) newPage() (*chromePage, error) {
	cd.cc.CheckChromeProcess()
	newPage, err := cd.cc.c.Target.CreateTarget(cd.ctx, target.NewCreateTargetArgs("about:blank"))
	if err != nil {
		return nil, err
	}

	newPageConn, err := cd.cc.mgr.Dial(cd.ctx, newPage.TargetID)
	if err != nil {
		return nil, err
	}
	newPageClient := cdp.NewClient(newPageConn)
	if err = runBatch(
		// Enable all the domain events that we're interested in.
		func() error { return newPageClient.DOM.Enable(cd.ctx) },
		func() error { return newPageClient.Network.Enable(cd.ctx, nil) },
		func() error { return newPageClient.Page.Enable(cd.ctx) },
		func() error { return newPageClient.Runtime.Enable(cd.ctx) },
	); err != nil {
		return nil, err
	}
	rspRcvd, err := newPageClient.Network.ResponseReceived(cd.ctx)
	if err != nil {
		return nil, err
	}
	return &chromePage{
		targetID: newPage.TargetID,
		conn:     newPageConn,
		client:   newPageClient,
		rspRcvd:  rspRcvd,
	}, nil
}

func (cd *ChromeDoer) closePage(p *chromePage) {
	p.conn.Close()
	cd.cc.c.Target.CloseTarget(cd.ctx, &target.CloseTargetArgs{p.targetID})
}

func (cd *ChromeDoer) activatePage(p *chromePage) error {
	return cd.cc.c.Target.ActivateTarget(cd.ctx, &target.ActivateTargetArgs{p.targetID})
}

func (cd *ChromeDoer) doNavigate(req *http.Request, keepAlive bool) (resp *http.Response, p *chromePage, err error) {
	var url = req.URL.String()
	log.Printf("Chrome doing: %v\n", url)

	newPage, err := cd.newPage()
	if err != nil {
		log.Println(err)
		return nil, nil, err
	}
	if keepAlive == false {
		defer cd.closePage(newPage)
	}

	domLoadTimer := time.After(cd.domLoadTime)
	err = navigate(cd.ctx, newPage.client.Page, url, cd.domLoadTime)
	if err != nil {
		log.Println(err)
		return nil, nil, err
	}
	<-domLoadTimer
	navhist, err := newPage.client.Page.GetNavigationHistory(cd.ctx)
	if err != nil {
		log.Printf("unable to get navHistory from Chrome: %v", err)
		return nil, nil, err
	}

	<-newPage.rspRcvd.Ready()
	rsp, err := newPage.rspRcvd.Recv()
	currUrl := navhist.Entries[navhist.CurrentIndex].URL
	log.Printf("Current URL: %v\n", currUrl)
	for {
		if err != nil {
			log.Println(err)
			return nil, nil, err
		}
		if rsp.Response.URL == currUrl || rsp.Response.URL+"/" == currUrl || rsp.Response.URL == currUrl+"/" {
			break
		}
		<-newPage.rspRcvd.Ready()
		rsp, err = newPage.rspRcvd.Recv()
	}

	doc, err := newPage.client.DOM.GetDocument(cd.ctx, nil)
	if err != nil {
		log.Println(err)
		return nil, nil, err
	}
	ohtml, err := newPage.client.DOM.GetOuterHTML(cd.ctx, &dom.GetOuterHTMLArgs{
		NodeID: &doc.Root.NodeID,
	})
	if err != nil {
		log.Println(err)
		return nil, nil, err
	}
	resp1 := &http.Response{
		StatusCode: int(rsp.Response.Status),
		Status:     rsp.Response.StatusText,
		Body:       ioutil.NopCloser(strings.NewReader(ohtml.OuterHTML)),
		Request:    req,
		Header:     make(http.Header),
	}
	rspHdrs, _ := rsp.Response.Headers.Map()
	for kk, vv := range rspHdrs {
		if strings.ToLower(kk) == "content-type" {
			resp1.Header.Set("Content-Type", vv)
			break
		}
	}
	return resp1, newPage, nil
}

func (cd *ChromeDoer) doJS(req *http.Request) (resp *http.Response, err error) {
	domLoadTimer := time.After(cd.domLoadTime)
	jscommand, err := url.PathUnescape(req.URL.Path)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	tgtUrl, err := url.QueryUnescape(req.URL.RawQuery)

	// TODO: fix below navhist check. fails because below test is
	// for first tab, which we don't use anymore.  replace with
	// KeepAlivePages.

	// navhist, _ := cd.cc.c.Page.GetNavigationHistory(cd.ctx)
	// currUrl := navhist.Entries[navhist.CurrentIndex].URL

	// if tgtUrl != currUrl && tgtUrl+"/" != currUrl && tgtUrl != currUrl+"/" {

	// 	domNavigateTimer := time.After(cd.domLoadTime)
	// 	log.Printf("Navigating to %v\n", tgtUrl)
	// 	tgtURL, err := url.Parse(tgtUrl)
	// 	if err != nil {
	// 		log.Printf("doJS: unable to parse tgt url: %v\n", err)
	// 		return nil, err
	// 	}

	// 	navRsp, err := cd.doNavigate(&http.Request{
	// 		URL: tgtURL,
	// 	})
	// 	if err != nil {
	// 		log.Printf("Navigation failed: %v\n", err)
	// 		return nil, err
	// 	}
	// 	if navRsp.StatusCode != 200 {
	// 		log.Printf("HTTP Status Code was: %v;  Use AddPage in chrome mode instead, to get page sent back anyway.")
	// 		return nil, err
	// 	}
	// 	<-domNavigateTimer
	// }

	tgtURL, err := url.Parse(tgtUrl)
	if err != nil {
		log.Printf("doJS: unable to parse tgt url: %v\n", err)
		return nil, err
	}

	navRsp, newPage, err := cd.doNavigate(&http.Request{
		URL: tgtURL,
	}, true)
	if err != nil {
		log.Printf("Navigation failed: %v\n", err)
		return nil, err
	}
	if navRsp.StatusCode != 200 {
		log.Printf("HTTP Status Code was: %v;  Use AddPage in chrome mode instead, to get page sent back anyway.", navRsp.StatusCode)
		log.Println(navRsp.Status)
		return nil, errors.New(navRsp.Status)
	}
	defer cd.closePage(newPage)

	if strings.HasSuffix(req.URL.Scheme, "builtinjs") {
		switch jscommand {
		case "/scrollToEnd":
			expression := `window.scrollTo(0, document.body.scrollHeight)`
			evalArgs := runtime.NewEvaluateArgs(expression)
			_, err := newPage.client.Runtime.Evaluate(cd.ctx, evalArgs)
			if err != nil {
				log.Println(err)
				return nil, err
			}
		case "/infiniteScrollToEnd":
			expression := `new Promise((resolve, reject) => {
                                   prevHeight=document.body.scrollHeight;
                                   window.scrollTo(0, document.body.scrollHeight);
                                   setTimeout(() => {
                                       newHeight=document.body.scrollHeight;
                                       resolve({"O": prevHeight, "N": newHeight});
                                   }, ` + strconv.Itoa(int(cd.domLoadTime/time.Millisecond)) + `);
                               });
`
			var count int32
			for {
				if cd.cc.scrollCount > 0 && cd.cc.scrollCount >= count {
					break
				}
				cd.cc.sema.Acquire(context.Background(), 1)
				evalArgs := runtime.NewEvaluateArgs(expression).SetAwaitPromise(true).SetReturnByValue(true)
				err := cd.activatePage(newPage) // to activate processing tab
				if err != nil {
					log.Println(err)
					return nil, err
				}
				eval, err := newPage.client.Runtime.Evaluate(cd.ctx, evalArgs)
				if err != nil {
					log.Println(err)
					return nil, err
				}
				cd.cc.sema.Release(1)
				heights := &struct {
					O int
					N int
				}{}
				if err = json.Unmarshal(eval.Result.Value, &heights); err != nil {
					log.Println(err)
					return nil, err
				}
				if heights.O == heights.N {
					log.Printf("Old height = new height = %v. We're probably done scrolling.\n", heights.O)
					break
				}
				log.Printf("Old height: %v; New height: %v; Continuing to scroll down.\n", heights.O, heights.N)
				count = count + 1
				time.Sleep(5 * time.Second)
			}
		}
	} else if strings.HasSuffix(req.URL.Scheme, "jscript") {
		evalArgs := runtime.NewEvaluateArgs(jscommand)
		_, err := newPage.client.Runtime.Evaluate(cd.ctx, evalArgs)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	}
	<-domLoadTimer
	doc, err := newPage.client.DOM.GetDocument(cd.ctx, nil)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	ohtml, err := newPage.client.DOM.GetOuterHTML(cd.ctx, &dom.GetOuterHTMLArgs{
		NodeID: &doc.Root.NodeID,
	})
	if err != nil {
		log.Println(err)
		return nil, err
	}
	navhist, _ := newPage.client.Page.GetNavigationHistory(cd.ctx)
	currURL, err := url.Parse(navhist.Entries[navhist.CurrentIndex].URL)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	req.URL = currURL
	resp1 := &http.Response{
		StatusCode: 900,
		Status:     "",
		Body:       ioutil.NopCloser(strings.NewReader(ohtml.OuterHTML)),
		Request:    req,
		Header:     make(http.Header),
	}
	resp1.Header.Set("Content-Type", "text/html")
	return resp1, nil
}

func navigate(ctx context.Context, pageClient cdp.Page, url string, timeout time.Duration) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()

	// Make sure Page events are enabled.
	err := pageClient.Enable(ctx)
	if err != nil {
		return err
	}

	// Open client for DOMContentEventFired to block until DOM has fully loaded.
	domContentEventFired, err := pageClient.DOMContentEventFired(ctx)
	if err != nil {
		return err
	}
	defer domContentEventFired.Close()

	_, err = pageClient.Navigate(ctx, page.NewNavigateArgs(url))
	if err != nil {
		return err
	}

	_, err = domContentEventFired.Recv()
	return err
}

// runBatchFunc is the function signature for runBatch.
type runBatchFunc func() error

// runBatch runs all functions simultaneously and waits until
// execution has completed or an error is encountered.
func runBatch(fn ...runBatchFunc) error {
	eg := errgroup.Group{}
	for _, f := range fn {
		eg.Go(f)
	}
	return eg.Wait()
}

func zmain() {
	cl := NewChromeClient("/usr/lib64/chromium-browser/headless_shell", "", true, 0)
	cl.Start()
	defer cl.Stop()
	urlobj, _ := url.Parse("http://books.toscrape.com/")
	req := &http.Request{
		URL: urlobj,
	}
	cd := cl.NewChromeDoer()
	r, err := cd.Do(req)
	pagebody, err := ioutil.ReadAll(r.Body)

	fmt.Println(err, string(pagebody))

	urlobj, _ = url.Parse("http://quotes.toscrape.com/")
	req = &http.Request{
		URL: urlobj,
	}
	r, err = cd.Do(req)
	pagebody, err = ioutil.ReadAll(r.Body)
	fmt.Println(err, string(pagebody))
}
