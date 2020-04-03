package main

import (
	"log"
	"time"

	gc "github.com/Suresh-Nakkeran/ideacrawler/goclient"
)

func main() {
	w, err := gc.NewWorker("127.0.0.1:2345", nil)
	if err != nil {
		log.Fatal(err)
	}

	spec := gc.NewJobSpec(
		gc.Depth(2),
		gc.MaxIdleTime(60),
		//		gc.Chrome(true, "/usr/bin/chromium"),
		gc.CallbackURLRegexp("author"),
		gc.FollowURLRegexp("com$|tag"),
		gc.PageChan(gc.NewPageChan()),
		gc.ThreadsPerSite(5),
	)

	z, err := gc.NewCrawlJob(w, spec)
	if err != nil {
		log.Fatal(err)
	}

	z.AddPage("http://quotes.toscrape.com", "")

	for z.IsAlive() == true {
		select {
		case ph := <-z.PageChan:
			log.Println(ph.Httpstatuscode, ph.Url, ph.UrlDepth)
		default:
			time.Sleep(time.Second)
		}
	}

}
