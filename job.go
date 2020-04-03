package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/PuerkitoBio/purell"
	"github.com/Suresh-Nakkeran/fetchbot"
	"github.com/Suresh-Nakkeran/ideacrawler/chromeclient"
	pb "github.com/Suresh-Nakkeran/ideacrawler/protofiles"
	sc "github.com/Suresh-Nakkeran/ideacrawler/statuscodes"
	"github.com/antchfx/xpath"
	htmlquery "github.com/antchfx/xquery/html"
	"github.com/gregjones/httpcache"
	"github.com/gregjones/httpcache/diskcache"
	"golang.org/x/sync/semaphore"
)

type job struct {
	domainname               string
	opts                     *pb.DomainOpt
	id                       pb.JobID
	seqnum                   int32
	callbackURLRegexp        *regexp.Regexp
	followURLRegexp          *regexp.Regexp
	callbackAnchorTextRegexp *regexp.Regexp
	subscriber               *subscriber
	mu                       sync.Mutex
	duplicates               map[string]bool
	cancelChan               chan struct{}
	cd                       *chromeclient.ChromeDoer

	doneChan chan struct{}
	randChan <-chan int // for random number between min and max norm distributed
	log      *log.Logger
}

func (j *job) sendPageHTML(ctx *fetchbot.Context, phtml pb.PageHTML) {
	if j.subscriber.connected == false {
		j.log.Println("Client not connected")
		return
	}

	if j.done() {
		j.log.Println("job has finished. can't send pages")
		return
	}
	select {
	case <-j.subscriber.stopChan:
		j.subscriber.connected = false
		if ctx != nil && j.opts.CancelOnDisconnect {
			j.log.Printf("Lost client, cancelling queue.\n")
			j.cancelChan <- struct{}{}
		} else {
			j.log.Printf("Lost client\n")
		}
		return
	case j.subscriber.sendChan <- phtml:
		return
	}
}

func (j *job) setupJobLogger(subID string) error {
	var logfile = "/dev/stdout"
	if cliParams.LogPath != "" {
		logfile = path.Join(cliParams.LogPath, "job-"+subID+".log")
	}
	logFP, err := os.Create(logfile)
	if err != nil {
		log.Printf("Unable to create logfile %v. not proceeding with job.", logfile)
		return err
	}
	j.log = log.New(logFP, subID+": ", log.LstdFlags|log.Lshortfile)
	return nil
}

func (j *job) fetchErrorHandler(ctx *fetchbot.Context, res *http.Response, err error) {
	if j.done() {
		return
	}
	j.log.Printf("ERR - fetch error : %s\n", err.Error())
	phtml := pb.PageHTML{
		Success: false,
		Error:   err.Error(),
		JobID:   &pb.JobID{},
		//TODO -- Check if ctx always has the correct URL
		Url:            ctx.Cmd.URL().String(),
		Httpstatuscode: sc.FetchbotError,
		Content:        []byte{},
		MetaStr:        ctx.Cmd.(crawlCommand).MetaStr(),
		UrlDepth:       ctx.Cmd.(crawlCommand).URLDepth(),
	}
	j.sendPageHTML(ctx, phtml)
	return
}

func (j *job) fetchHTTPGetHandler(ctx *fetchbot.Context, res *http.Response, err error) {
	if j.done() {
		return
	}
	requestURL := res.Request.URL
	ccmd := ctx.Cmd.(crawlCommand)
	anchorText := ccmd.anchorText
	if ccmd.noCallback == true {
		return
	}
	if res.StatusCode > 399 && res.StatusCode < 600 {
		j.log.Printf("STATUS - %s %s - %d : %s\n", ctx.Cmd.Method(), ctx.Cmd.URL(), res.StatusCode, res.Status)
		if ccmd.URLDepth() == 0 {
			phtml := pb.PageHTML{
				Success:        false,
				Error:          res.Status,
				JobID:          &pb.JobID{},
				Url:            requestURL.String(),
				Httpstatuscode: int32(res.StatusCode),
				Content:        []byte{},
				MetaStr:        ccmd.MetaStr(),
				UrlDepth:       ccmd.URLDepth(),
			}
			j.sendPageHTML(ctx, phtml)
		}
		return
	}
	pageBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		emsg := fmt.Sprintf("ERR - %s %s - %s", ctx.Cmd.Method(), ctx.Cmd.URL(), err)
		j.log.Println(emsg)
		phtml := pb.PageHTML{
			Success:        false,
			Error:          emsg,
			JobID:          &pb.JobID{},
			Url:            requestURL.String(),
			Httpstatuscode: sc.ResponseError,
			MetaStr:        ctx.Cmd.(crawlCommand).MetaStr(),
			Content:        []byte{},
			UrlDepth:       ccmd.URLDepth(),
		}
		j.sendPageHTML(ctx, phtml)
		return
	}

	// check if still logged in
	if j.opts.Login && j.opts.CheckLoginAfterEachPage && j.opts.LoginSuccessCheck != nil {
		doc, _ := htmlquery.Parse(bytes.NewBuffer(pageBody))
		expr, _ := xpath.Compile(j.opts.LoginSuccessCheck.Key) // won't lead to error now, because this is the second time this is happening.

		iter := expr.Evaluate(htmlquery.CreateXPathNavigator(doc)).(*xpath.NodeIterator)
		iter.MoveNext()
		if strings.ToLower(iter.Current().Value()) != strings.ToLower(j.opts.LoginSuccessCheck.Value) {
			errMsg := fmt.Sprintf("Not logged in anymore: In xpath '%s', expected '%s', but got '%s'. Not continuing.", j.opts.LoginSuccessCheck.Key, j.opts.LoginSuccessCheck.Value, iter.Current().Value())
			j.log.Println(errMsg)
			phtml := pb.PageHTML{
				Success:        false,
				Error:          errMsg,
				JobID:          nil, //no subscription object
				Url:            "",
				Httpstatuscode: sc.NolongerLoggedIn,
				Content:        []byte{},
				UrlDepth:       ccmd.URLDepth(),
			}
			j.sendPageHTML(ctx, phtml)
			j.cancelChan <- struct{}{}
			if cliParams.SaveLoginPages != "" {
				err := ioutil.WriteFile(path.Join(cliParams.SaveLoginPages, "loggedout.html"), pageBody, 0755)
				if err != nil {
					j.log.Println("ERR - unable to save loggedout file:", err)
				}
			}
			return
		}
	}

	// Enqueue all links as HEAD requests, if they match followUrlRegexp
	if j.opts.NoFollow == false && (j.followURLRegexp == nil || j.followURLRegexp.MatchString(ctx.Cmd.URL().String()) == true) && (j.opts.Depth < 0 || ccmd.URLDepth() < j.opts.Depth) {
		// Process the body to find the links
		doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(pageBody)))
		if err != nil {
			emsg := fmt.Sprintf("ERR - %s %s - %s\n", ctx.Cmd.Method(), ctx.Cmd.URL(), err)
			j.log.Println(emsg)
			phtml := pb.PageHTML{
				Success:        false,
				Error:          emsg,
				JobID:          &pb.JobID{},
				Url:            "",
				Httpstatuscode: sc.FollowParseError,
				Content:        []byte{},
				UrlDepth:       ccmd.URLDepth(),
			}
			j.sendPageHTML(ctx, phtml)
			return
		}
		j.enqueueLinks(ctx, doc, ccmd.URLDepth()+1, requestURL)
		j.log.Println("Enqueued", ctx.Cmd.URL().String())
	}

	var callbackPage = false

	// Callback if depth = 0
	if j.opts.CallbackSeedUrl == true && ccmd.URLDepth() == 0 {
		callbackPage = true
	}

	if len(j.opts.CallbackUrlRegexp) == 0 && len(j.opts.CallbackXpathMatch) == 0 && len(j.opts.CallbackXpathRegexp) == 0 {
		callbackPage = true
	}

	if j.callbackURLRegexp != nil && j.callbackURLRegexp.MatchString(ctx.Cmd.URL().String()) == false {
		j.log.Printf("Url '%v' did not match callbackRegexp '%v'\n", ctx.Cmd.URL().String(), j.callbackURLRegexp)
	} else if j.callbackURLRegexp != nil {
		callbackPage = true
	}

	if j.callbackAnchorTextRegexp != nil && j.callbackAnchorTextRegexp.MatchString(anchorText) == false {
		j.log.Printf("Anchor Text '%v' did not match anchorTextRegexp '%v'\n", anchorText, j.callbackAnchorTextRegexp)
	} else if j.callbackAnchorTextRegexp != nil {
		callbackPage = true
	}

	if callbackPage == false && len(j.opts.CallbackXpathMatch) > 0 {
		passthru := true
		doc, _ := htmlquery.Parse(bytes.NewBuffer(pageBody))
		for _, xm := range j.opts.CallbackXpathMatch {
			expr, _ := xpath.Compile(xm.Key)

			iter := expr.Evaluate(htmlquery.CreateXPathNavigator(doc)).(*xpath.NodeIterator)
			iter.MoveNext()
			if iter.Current().Value() != xm.Value {
				passthru = false
				j.log.Printf("Url '%v' did not have '%v' at xpath '%v'\n", ctx.Cmd.URL().String(), xm.Value, xm.Key)
				break
			}
		}
		if passthru {
			callbackPage = true
		}
	}

	if callbackPage == false && len(j.opts.CallbackXpathRegexp) > 0 {
		passthru := true
		doc, _ := htmlquery.Parse(bytes.NewBuffer(pageBody))
		for _, xm := range j.opts.CallbackXpathRegexp {
			expr, _ := xpath.Compile(xm.Key)

			iter := expr.Evaluate(htmlquery.CreateXPathNavigator(doc)).(*xpath.NodeIterator)
			iter.MoveNext()
			if iter.Current().Value() != xm.Value {
				passthru = false
				j.log.Printf("Url '%v' did not have '%v' at xpath '%v'\n", ctx.Cmd.URL().String(), xm.Value, xm.Key)
				break
			}
		}
		if passthru {
			callbackPage = true
		}
	}

	if callbackPage == false {
		return // no need to send page back to client.
	}
	shippingURL := ""
	if strings.HasPrefix(ctx.Cmd.URL().Scheme, "crawljs") == true {
		shippingURL, err = url.QueryUnescape(ctx.Cmd.URL().RawQuery)
		if err != nil {
			j.log.Printf("Unable to UnEscape RawQuery - %v, err: %v\n", ctx.Cmd.URL().RawQuery, err)
			shippingURL = ctx.Cmd.URL().String()
		}
	} else {
		shippingURL = ctx.Cmd.URL().String()
	}
	j.log.Println("Shipping", shippingURL)

	sub := j.id

	phtml := pb.PageHTML{
		Success:        true,
		Error:          "",
		JobID:          &sub,
		Url:            shippingURL,
		Httpstatuscode: int32(res.StatusCode),
		Content:        pageBody,
		MetaStr:        ccmd.MetaStr(),
		UrlDepth:       ccmd.URLDepth(),
		AnchorText:     anchorText,
	}
	j.sendPageHTML(ctx, phtml)
}

func (j *job) fetchHTTPHeadHandler(ctx *fetchbot.Context, res *http.Response, err error) {
	if j.done() {
		return
	}
	cmd := crawlCommand{
		method:     "GET",
		url:        ctx.Cmd.URL(),
		noCallback: false,
		metaStr:    ctx.Cmd.(crawlCommand).MetaStr(),
	}
	if err := ctx.Q.Send(cmd); err != nil {
		j.log.Printf("ERR - %s %s - %s\n", ctx.Cmd.Method(), ctx.Cmd.URL(), err)
	}
}

func (j *job) makeNetworkTransport() http.RoundTripper {
	var networkTransport http.RoundTripper
	if j.opts.NetworkIface != "" && cliParams.DialAddress != "" {
		localAddress := &net.TCPAddr{
			IP:   net.ParseIP(cliParams.DialAddress), // a secondary local IP I assigned to me
			Port: 80,
		}
		networkTransport = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
				LocalAddr: localAddress,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
	}
	return networkTransport
}

func (s *ideaCrawlerWorker) makeHTTPClientRawDirect(j *job, networkTransport http.RoundTripper) (fetchbot.Doer, error) {
	gCurCookieJar, _ := cookiejar.New(nil)
	httpClient := &http.Client{
		Transport:     networkTransport,
		CheckRedirect: nil,
		Jar:           gCurCookieJar,
	}
	if j.opts.Prefetch == true {
		httpClient.Transport = &httpcache.Transport{
			Transport:           networkTransport,
			Cache:               diskcache.New("/tmp/ideacache/" + j.id.ID),
			MarkCachedResponses: true,
		}
	}
	return &Doer{httpClient, j, semaphore.NewWeighted(int64(j.opts.MaxConcurrentRequests)), s}, nil
}

func (s *ideaCrawlerWorker) makeHTTPClientLoginDirect(j *job, networkTransport http.RoundTripper) (fetchbot.Doer, error) {
	// create our own httpClient and attach a cookie jar to it,
	// login using that client to the site if required,
	// check if login succeeded,
	// then give that client to fetchbot's fetcher.
	gCurCookieJar, _ := cookiejar.New(nil)
	httpClient := &http.Client{
		Transport:     networkTransport,
		CheckRedirect: nil,
		Jar:           gCurCookieJar,
	}
	if j.opts.Prefetch == true {
		httpClient.Transport = &httpcache.Transport{
			Transport:           networkTransport,
			Cache:               diskcache.New("/tmp/ideacache/" + j.id.ID),
			MarkCachedResponses: true,
		}
	}
	var payload = make(url.Values)
	if j.opts.LoginParseFields == true {
		afterTime := j.opts.MinDelay
		if afterTime < 1 {
			afterTime = 1
		}
		if j.opts.MaxDelay > j.opts.MinDelay {
			afterTime = int32(<-j.randChan)
			j.log.Printf("Next delay - %v\n", time.Duration(afterTime)*time.Second)
		}
		after := time.After(time.Duration(afterTime) * time.Second)

		httpReq, err := http.NewRequest("GET", j.opts.LoginUrl, nil)
		if err != nil {
			j.log.Println(err)
			return nil, err
		}
		httpReq.Header.Set("User-Agent", j.opts.Useragent)

		httpResp, err := httpClient.Do(httpReq)
		if err != nil {
			j.log.Println(err)
			return nil, err
		}
		pageBody, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			j.log.Println("Unable to read http response:", err)
			return nil, err
		}
		if cliParams.SaveLoginPages != "" {
			err := ioutil.WriteFile(path.Join(cliParams.SaveLoginPages, "login.html"), pageBody, 0755)
			if err != nil {
				j.log.Println("ERR - Unable to save login file:", err)
			}
		}

		doc, _ := htmlquery.Parse(bytes.NewBuffer(pageBody))
		for _, kvp := range j.opts.LoginParseXpath {
			expr, err := xpath.Compile(kvp.Value)
			if err != nil {
				j.log.Println(err)
				return nil, err
			}

			iter := expr.Evaluate(htmlquery.CreateXPathNavigator(doc)).(*xpath.NodeIterator)
			iter.MoveNext()
			payload.Set(kvp.Key, iter.Current().Value())
		}
		<-after
	}

	for _, kvp := range j.opts.LoginPayload {
		payload.Set(kvp.Key, kvp.Value)
	}
	httpResp, err := httpClient.PostForm(j.opts.LoginUrl, payload)
	if err != nil {
		j.log.Println(err)
		return nil, err
	}
	pageBody, err := ioutil.ReadAll(httpResp.Body)
	if cliParams.SaveLoginPages != "" {
		err := ioutil.WriteFile(path.Join(cliParams.SaveLoginPages, "loggedin.html"), pageBody, 0755)
		if err != nil {
			j.log.Println("ERR - Unable to save loggedin file:", err)
		}

	}
	doc, _ := htmlquery.Parse(bytes.NewBuffer(pageBody))
	if j.opts.LoginSuccessCheck != nil {
		expr, err := xpath.Compile(j.opts.LoginSuccessCheck.Key)
		if err != nil {
			j.log.Println(err)
			return nil, err
		}
		iter := expr.Evaluate(htmlquery.CreateXPathNavigator(doc)).(*xpath.NodeIterator)
		iter.MoveNext()
		if strings.ToLower(iter.Current().Value()) != strings.ToLower(j.opts.LoginSuccessCheck.Value) {
			errMsg := fmt.Sprintf("Login failed: In xpath '%s', expected '%s', but got '%s'. Not proceeding.", j.opts.LoginSuccessCheck.Key, j.opts.LoginSuccessCheck.Value, iter.Current().Value())
			j.log.Println(errMsg)
			phtml := pb.PageHTML{
				Success:        false,
				Error:          errMsg,
				JobID:          nil, //no subscription object
				Url:            "",
				Httpstatuscode: sc.LoginFailed,
				Content:        []byte{},
			}
			j.sendPageHTML(nil, phtml)
			return nil, err
		}
		phtml := pb.PageHTML{
			Success:        true,
			Error:          "",
			JobID:          nil, //no subscription object
			Url:            "",
			Httpstatuscode: sc.LoginSuccess,
			Content:        []byte{},
		}
		j.sendPageHTML(nil, phtml)
		j.log.Printf("Logged in. Found '%v' in '%v'\n", j.opts.LoginSuccessCheck.Value, j.opts.LoginSuccessCheck.Key)
	}
	return &Doer{httpClient, j, semaphore.NewWeighted(int64(j.opts.MaxConcurrentRequests)), s}, nil
}

func (s *ideaCrawlerWorker) setupChromeClient(chromeBinary string, userAgent string, disableImages bool, scrollCount int32) {
	if s.ccl == nil {
		s.ccl = chromeclient.NewChromeClient(chromeBinary, userAgent, disableImages, scrollCount)
		s.ccl.CheckChromeProcess()
	}
}

func (s *ideaCrawlerWorker) makeHTTPClientRawChrome(j *job) (fetchbot.Doer, error) {
	j.opts.Impolite = true // Always impolite in Chrome mode.
	if j.opts.ChromeBinary == "" {
		j.opts.ChromeBinary = "/usr/lib64/chromium-browser/headless_shell"
	}

	s.setupChromeClient(j.opts.ChromeBinary, j.opts.Useragent, j.opts.DisableImages, j.opts.ScrollCount)
	j.cd = s.ccl.NewChromeDoer()
	if j.opts.DomLoadTime > 0 {
		j.cd.SetDomLoadTime(j.opts.DomLoadTime)
	}
	return &Doer{j.cd, j, semaphore.NewWeighted(int64(j.opts.MaxConcurrentRequests)), s}, nil
}

func (s *ideaCrawlerWorker) makeHTTPClientLoginChrome(j *job) (fetchbot.Doer, error) {
	j.opts.Impolite = true // Always impolite in Chrome mode.
	if j.opts.ChromeBinary == "" {
		j.opts.ChromeBinary = "/usr/lib64/chromium-browser/headless_shell"
	}
	s.setupChromeClient(j.opts.ChromeBinary, j.opts.Useragent, j.opts.DisableImages, j.opts.ScrollCount)
	j.cd = s.ccl.NewChromeDoer()
	if j.opts.DomLoadTime > 0 {
		j.cd.SetDomLoadTime(j.opts.DomLoadTime)
	}

	urlobj, _ := url.Parse(j.opts.LoginUrl)
	req := &http.Request{
		URL: urlobj,
	}
	loginResp, err := j.cd.Do(req)
	if err != nil {
		j.log.Println("Http request to chrome failed:", err)
		return nil, err
	}
	loginBody, err := ioutil.ReadAll(loginResp.Body)
	if err != nil {
		j.log.Println("Unable to read http response", err)
		return nil, err
	}
	if cliParams.SaveLoginPages != "" {
		err := ioutil.WriteFile(path.Join(cliParams.SaveLoginPages, "login.html"), loginBody, 0755)
		if err != nil {
			j.log.Println("ERR - Unable to save login file:", err)
		}
	}
	loginjs := &url.URL{
		Scheme:   "crawljs-jscript",
		Host:     j.opts.LoginUrl,
		Path:     url.PathEscape(j.opts.LoginJS), // js string
		RawQuery: url.QueryEscape(j.opts.LoginUrl),
	}
	loginJsReq := &http.Request{
		URL: loginjs,
	}
	loggedInResp, err := j.cd.Do(loginJsReq)
	loggedInBody, err := ioutil.ReadAll(loggedInResp.Body)
	if cliParams.SaveLoginPages != "" {
		err := ioutil.WriteFile(path.Join(cliParams.SaveLoginPages, "loggedin.html"), loggedInBody, 0755)
		if err != nil {
			j.log.Println("ERR - Unable to save loggedin file:", err)
		}

	}
	doc, _ := htmlquery.Parse(bytes.NewBuffer(loggedInBody))
	if j.opts.LoginSuccessCheck != nil {
		expr, err := xpath.Compile(j.opts.LoginSuccessCheck.Key)
		if err != nil {
			j.log.Printf("Unable to compile xpath - %v; err:%v\n", j.opts.LoginSuccessCheck.Key, err)
			return nil, err
		}
		iter := expr.Evaluate(htmlquery.CreateXPathNavigator(doc)).(*xpath.NodeIterator)
		iter.MoveNext()
		if iter.Current().Value() != j.opts.LoginSuccessCheck.Value {
			errMsg := fmt.Sprintf("Login failed: In xpath '%s', expected '%s', but got '%s'. Not proceeding.", j.opts.LoginSuccessCheck.Key, j.opts.LoginSuccessCheck.Value, iter.Current().Value())
			j.log.Println(errMsg)
			phtml := pb.PageHTML{
				Success:        false,
				Error:          errMsg,
				JobID:          nil, //no subscription object
				Url:            "",
				Httpstatuscode: sc.LoginFailed,
				Content:        []byte{},
			}
			j.sendPageHTML(nil, phtml)
			return nil, errors.New(errMsg)
		}
		phtml := pb.PageHTML{
			Success:        true,
			Error:          "",
			JobID:          nil, //no subscription object
			Url:            "",
			Httpstatuscode: sc.LoginSuccess,
			Content:        []byte{},
		}
		j.sendPageHTML(nil, phtml)
		j.log.Printf("Logged in. Found '%v' in '%v'\n", j.opts.LoginSuccessCheck.Value, j.opts.LoginSuccessCheck.Key)
	}
	return &Doer{j.cd, j, semaphore.NewWeighted(int64(j.opts.MaxConcurrentRequests)), s}, nil
}

func (j *job) done() bool {
	select {
	case _, ok := <-j.doneChan:
		if ok == false {
			return true
		}
	default:
		return false
	}
	return false // never reached
}

func (s *ideaCrawlerWorker) RunJob(subID string, j *job) {
	err := j.setupJobLogger(subID)
	if err != nil {
		log.Printf("setupJobLogger failed: %s", err)
		return
	}
	j.log.Println("starting job -", subID)
	defer func() {
		close(j.subscriber.sendChan)
		select {
		case <-j.doneChan:
		default:
			close(j.doneChan)
		}
		j.log.Println("stopping job -", subID)
	}()

	mux := fetchbot.NewMux()
	mux.HandleErrors(fetchbot.HandlerFunc(j.fetchErrorHandler))
	mux.Response().Method("GET").ContentType("text/html").Handler(fetchbot.HandlerFunc(j.fetchHTTPGetHandler))
	mux.Response().Method("HEAD").ContentType("text/html").Handler(fetchbot.HandlerFunc(j.fetchHTTPHeadHandler))

	f := fetchbot.New(mux)
	j.log.Printf("MaxConcurrentRequests=%v\n", int64(j.opts.MaxConcurrentRequests))

	var networkTransport = j.makeNetworkTransport()

	if s.mode == modeServer && j.opts.Chrome == false {
		f.HttpClient = &Doer{s.wm, j, semaphore.NewWeighted(int64(j.opts.MaxConcurrentRequests)), s}
	} else if s.mode == modeServer && j.opts.Chrome == true {
		f.HttpClient = &Doer{s.cwm, j, semaphore.NewWeighted(int64(j.opts.MaxConcurrentRequests)), s}
	} else if j.opts.Chrome == false && j.opts.Login == false {
		f.HttpClient, err = s.makeHTTPClientRawDirect(j, networkTransport)
		if err != nil {
			return
		}
	} else if j.opts.Chrome == false && j.opts.Login == true {
		f.HttpClient, err = s.makeHTTPClientLoginDirect(j, networkTransport)
		if err != nil {
			return
		}
	} else if j.opts.Chrome == true && j.opts.Login == false {
		f.HttpClient, err = s.makeHTTPClientRawChrome(j)
		if err != nil {
			return
		}
	} else if j.opts.Chrome == true && j.opts.Login == true {
		f.HttpClient, err = s.makeHTTPClientLoginChrome(j)
		if err != nil {
			return
		}
	}

	f.DisablePoliteness = j.opts.Impolite
	// minimal crawl delay; actual randomized delay is implemented in IdeaCrawlDoer's Do method.
	f.CrawlDelay = 50 * time.Millisecond
	if j.opts.MaxIdleTime == 0 {
		f.WorkerIdleTTL = 600 * time.Second
	} else if j.opts.MaxIdleTime < 10 {
		f.WorkerIdleTTL = 10 * time.Second
	} else {
		f.WorkerIdleTTL = time.Duration(j.opts.MaxIdleTime) * time.Second
	}
	f.AutoClose = true
	f.UserAgent = j.opts.Useragent
	f.ThreadsPerSite = j.opts.ThreadsPerSite
	//TODO: hipri: create goroutine to listen for new PageRequest objects

	q := f.Start()

	// handle cancel requests
	go func() {
		select {
		case <-j.doneChan:
			return
		case <-j.subscriber.stopChan:
			close(j.doneChan)
			q.Cancel()
			j.log.Println("Cancelled job:", subID)
		case <-j.cancelChan:
			close(j.doneChan)
			q.Cancel()
			j.log.Println("Cancelled job:", subID)
		}
	}()

	// handle stuff coming through the addPage function
	go func() {
	handlePagesLoop:
		for {
			select {
			case pr := <-j.subscriber.reqChan: // TODO: No URL normalization if added through this method?
				switch pr.Reqtype {
				case pb.PageReqType_GET:
					// TODO:  add error checking for error from Send functions
					cmd, err := newCrawlCommand("GET", pr.Url, pr.MetaStr, 0)
					if err != nil {
						j.log.Println(err)
						return
					}
					err = q.Send(cmd)
					if err != nil {
						j.log.Println(err)
						return
					}
				case pb.PageReqType_HEAD:
					cmd, err := newCrawlCommand("HEAD", pr.Url, pr.MetaStr, 0)
					if err != nil {
						j.log.Println(err)
						return
					}
					err = q.Send(cmd)
					if err != nil {
						j.log.Println(err)
						return
					}
				case pb.PageReqType_BUILTINJS:
					prURL, err := url.Parse(pr.Url)
					if err != nil {
						j.log.Println(err)
						return
					}
					cmd := crawlCommand{
						method: "GET",
						url: &url.URL{
							Scheme:   "crawljs-builtinjs",
							Host:     prURL.Host,
							Path:     url.PathEscape(pr.Js), // url command name
							RawQuery: url.QueryEscape(pr.Url),
						},
						noCallback: pr.NoCallback,
						metaStr:    pr.MetaStr,
						urlDepth:   0,
					}
					err = q.Send(cmd)
					if err != nil {
						j.log.Println(err)
						return
					}
				case pb.PageReqType_JSCRIPT:
					prURL, err := url.Parse(pr.Url)
					if err != nil {
						j.log.Println(err)
						return
					}
					cmd := crawlCommand{
						method: "GET",
						url: &url.URL{
							Scheme:   "crawljs-jscript",
							Host:     prURL.Host,
							Path:     url.PathEscape(pr.Js), // url command name
							RawQuery: url.QueryEscape(pr.Url),
						},
						noCallback: pr.NoCallback,
						metaStr:    pr.MetaStr,
						urlDepth:   0,
					}
					err = q.Send(cmd)
					if err != nil {
						j.log.Println(err)
						return
					}
				}
				j.log.Println("Enqueued page:", pr.Url)
			case <-j.doneChan:
				break handlePagesLoop
			}
		}
	}()
	if len(j.opts.SeedUrl) > 0 {
		j.mu.Lock()
		j.duplicates[j.opts.SeedUrl] = true
		j.mu.Unlock()
		cmd, err := newCrawlCommand("GET", j.opts.SeedUrl, "", 0)
		if err != nil {
			j.log.Println(err)
			return
		}
		err = q.Send(cmd)
		if err != nil {
			j.log.Println(err)
			return
		}
	}
	q.Block()
}

func (j *job) checkEnqueueEligibility(nurl string, anchorText string) bool {
	var reqMatch = true
	var followMatch = true

	if (j.callbackURLRegexp != nil && j.callbackURLRegexp.MatchString(nurl) == false) || (j.callbackAnchorTextRegexp != nil && j.callbackAnchorTextRegexp.MatchString(anchorText) == false) {
		reqMatch = false
	}
	if j.followURLRegexp != nil && j.followURLRegexp.MatchString(nurl) == false {
		followMatch = false
	}
	if !reqMatch && !followMatch {
		return false
	}
	return true
}

func (j *job) enqueueLinks(ctx *fetchbot.Context, doc *goquery.Document, urlDepth int32, requestURL *url.URL) {
	j.mu.Lock()
	defer j.mu.Unlock()
	var SendMethod = "GET"
	if j.opts.CheckContent == true {
		SendMethod = "HEAD"
	}
	var urlMap = make(map[string]bool)
	doc.Find("a[href]").Each(func(i int, s *goquery.Selection) {
		val, _ := s.Attr("href")
		anchorText := strings.TrimSpace(s.Text())

		// Resolve address
		u, err := requestURL.Parse(val)
		if err != nil {
			j.log.Printf("enqueuelinks: resolve URL %s - %s\n", val, err)
			return
		}
		normFlags := purell.FlagsSafe
		if j.opts.UnsafeNormalizeURL == true {
			normFlags = purell.FlagsSafe | purell.FlagRemoveFragment | purell.FlagRemoveDirectoryIndex
			// Remove query params
			u.RawQuery = ""
		}
		nurl := purell.NormalizeURL(u, normFlags)
		if j.subscriber.analyzedURLConnected == true {
			urlMap[nurl] = true
		}

		if j.checkEnqueueEligibility(nurl, anchorText) == false {
			return
		}

		if !j.duplicates[nurl] {
			if j.opts.SeedUrl != "" && (!j.opts.FollowOtherDomains && u.Hostname() != j.domainname) {
				j.duplicates[nurl] = true
				return
			}
			parsedURL, err := url.Parse(nurl)
			if err != nil {
				j.log.Println(err)
				return

			}

			cmd := crawlCommand{
				method:     SendMethod,
				url:        parsedURL,
				metaStr:    ctx.Cmd.(crawlCommand).MetaStr(),
				urlDepth:   urlDepth,
				anchorText: anchorText,
			}
			//cmd, err := CreateCommand(SendMethod, nurl, "", urlDepth)
			//if err != nil {
			//	job.log.Println(err)
			//	return
			//}
			j.log.Printf("Enqueueing URL: %s", nurl)
			if err := ctx.Q.Send(cmd); err != nil {
				j.log.Printf("error: enqueue head %s - %s", nurl, err)
			} else {
				j.duplicates[nurl] = true
			}
		}
	})

	if j.subscriber.analyzedURLConnected == true {
		if j.subscriber.connected == false {
			return
		}

		urlList := pb.UrlList{}
		for url := range urlMap {
			urlList.Url = append(urlList.Url, url)
		}
		urlList.MetaStr = ctx.Cmd.(crawlCommand).MetaStr()
		urlList.UrlDepth = urlDepth

		select {
		case <-j.subscriber.stopAnalyzedURLChan:
			j.subscriber.analyzedURLConnected = false
			return
		case j.subscriber.analyzedURLChan <- urlList:
			return
		}
	}
}
