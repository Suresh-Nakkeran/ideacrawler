package main

import (
	"log"
	"math/rand"
	"regexp"
	"strconv"
	"testing"
	"time"

	gc "github.com/Suresh-Nakkeran/ideacrawler/goclient"
	"github.com/phayes/freeport"
)

var serverRunning bool
var serverPort string

func startTestServer() (string, error) {
	if serverRunning == true {
		return serverPort, nil
	}
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	rand.Seed(time.Now().UTC().UnixNano())
	port, err := freeport.GetFreePort()
	if err != nil {
		return "", err
	}
	serverPort = strconv.Itoa(port)
	cliParams.ClientListenAddress = "127.0.0.1:" + serverPort
	go startCrawlerWorker(modeStandalone, newServer(modeStandalone, nil))
	serverRunning = true
	time.Sleep(1 * time.Second)
	return serverPort, nil
}

type TestData struct {
	name              string
	seedURL           string
	chrome            bool
	maxIdleTime       int64
	depth             int32
	followURLRegexp   string
	callbackURLRegexp string
	respStatusCode    int32
	respBodyRegexp    string
	respBodyMinLength int
	respPageCount     int
	pages             []string
}

func TestSinglePage(t *testing.T) {
	port, err := startTestServer()

	if err != nil {
		t.Fatal("unable to start test server:", err)
	}

	var dataPoints = []TestData{
		// TestData{
		// 	name:              "chrome200",
		// 	seedURL:           "http://quotes.toscrape.com/tag/simile/",
		// 	chrome:            true,
		// 	respStatusCode:    200,
		// 	respBodyRegexp:    "Life.is.like.riding.a.bicycle",
		// 	respBodyMinLength: 100,
		// },
		// TestData{
		// 	name:              "chrome404",
		// 	seedURL:           "http://quotes.toscrape.com/tag/simile/1",
		// 	chrome:            true,
		// 	respStatusCode:    404,
		// 	respBodyRegexp:    "",
		// 	respBodyMinLength: 0,
		// },
		TestData{
			name:              "simple200",
			seedURL:           "http://quotes.toscrape.com/tag/simile/",
			chrome:            false,
			respStatusCode:    200,
			respBodyRegexp:    "Life.is.like.riding.a.bicycle",
			respBodyMinLength: 100,
		},
		TestData{
			name:              "simple404",
			seedURL:           "http://quotes.toscrape.com/tag/simile/1",
			chrome:            false,
			respStatusCode:    404,
			respBodyRegexp:    "",
			respBodyMinLength: 0,
		},
	}

	for _, dp := range dataPoints {
		t.Run(dp.name, func(t *testing.T) {
			w, err := gc.NewWorker("127.0.0.1:"+port, nil)
			if err != nil {
				t.Fatal(err)
			}

			spec := gc.NewJobSpec(
				gc.SeedURL(dp.seedURL),
				gc.Chrome(dp.chrome, "/usr/bin/chromium"),
				gc.NoFollow(),
				gc.PageChan(gc.NewPageChan()),
			)

			z, err := gc.NewCrawlJob(w, spec)
			if err != nil {
				t.Fatal(err)
			}

			var ph *gc.PageHTML
			var ok bool
			if ph, ok = <-z.PageChan; ok == false {
				t.Fatalf("job ended early")
			}

			if ph.Httpstatuscode != dp.respStatusCode {
				t.Errorf("expected status code: %d, got %d.", dp.respStatusCode, ph.Httpstatuscode)
			}
			if dp.respBodyMinLength > 0 && len(string(ph.Content)) < dp.respBodyMinLength {
				t.Errorf("expected respBodyMinLength of %d, got %d.", dp.respBodyMinLength, len(string(ph.Content)))
			}
			if dp.respBodyRegexp != "" {
				re := regexp.MustCompile(dp.respBodyRegexp)
				if re.Match(ph.Content) == false {
					t.Errorf("regexp match failed for: %s", dp.respBodyRegexp)
				}
			}
			z.Stop()
		})
	}
}

func TestMultiSiteFilter(t *testing.T) {
	port, err := startTestServer()

	if err != nil {
		t.Fatal("unable to start test server:", err)
	}

	var dataPoints = []TestData{
		// TestData{
		// 	name:              "depth1Chrome",
		// 	seedURL:           "",
		// 	chrome:            true,
		// 	followUrlRegexp:   "com$|author",
		// 	callbackUrlRegexp: "hub|author",
		// 	respStatusCode:    200,
		// 	respBodyMinLength: 100,
		// 	depth:             1,
		// 	maxIdleTime:       10,
		// 	pages:             []string{"http://quotes.toscrape.com", "http://books.toscrape.com"},
		// 	respPageCount:     9,
		// },
		TestData{
			name:              "depth1Raw",
			seedURL:           "",
			chrome:            false,
			followURLRegexp:   "com$|author",
			callbackURLRegexp: "hub|author",
			respStatusCode:    200,
			respBodyMinLength: 100,
			depth:             1,
			maxIdleTime:       10,
			pages:             []string{"http://quotes.toscrape.com", "http://books.toscrape.com"},
			respPageCount:     9,
		},
	}

	for _, dp := range dataPoints {
		t.Run(dp.name, func(t *testing.T) {
			w, err := gc.NewWorker("127.0.0.1:"+port, nil)
			if err != nil {
				t.Fatal(err)
			}

			spec := gc.NewJobSpec(
				gc.SeedURL(dp.seedURL),
				gc.Chrome(dp.chrome, "/usr/bin/chromium"),
				gc.Depth(dp.depth),
				gc.MinDelay(1),
				gc.Impolite(),
				gc.MaxIdleTime(dp.maxIdleTime),
				gc.CallbackURLRegexp(dp.callbackURLRegexp),
				gc.FollowURLRegexp(dp.followURLRegexp),
				gc.PageChan(gc.NewPageChan()),
			)

			z, err := gc.NewCrawlJob(w, spec)
			if err != nil {
				t.Fatal(err)
			}

			for _, ii := range dp.pages {
				z.AddPage(ii, "")
			}

			pageCount := 0
			for z.IsAlive() == true { // TODO:  or add timeout.
				select {
				case ph := <-z.PageChan:
					if ph.Httpstatuscode != dp.respStatusCode {
						t.Errorf("expected status code: %d, got %d.", dp.respStatusCode, ph.Httpstatuscode)
					}
					pageCount++
				default:
					time.Sleep(1 * time.Second)
				}
			}
			if pageCount != dp.respPageCount {
				t.Errorf("expected %d response pages, got %d", dp.respPageCount, pageCount)
			}
		})
	}
}
