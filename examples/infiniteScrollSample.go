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

package main

import (
	"fmt"
	"io/ioutil"
	"time"

	gc "github.com/Suresh-Nakkeran/ideacrawler/goclient"
)

func main() {

	z := gc.NewCrawlJob("127.0.0.1", "2345")

	// Enables callback through channel. Default is callback through function.
	z.UsePageChan = true

	// Remove fragments in the crawled urls.
	z.UnsafeNormalizeURL = false
	z.Impolite = true
	z.CancelOnDisconnect = true

	// Time delay between each page crawling. Time delay will be randomly generated between MinDelay and MaxDelay(in Seconds).
	z.MinDelay = 1
	z.MaxDelay = 5

	//Need to enable chrome or not.
	z.ChromeBinary = "/bin/chrome"
	z.Chrome = true
	z.SeedURL = ""

	z.Start()

	z.AddJS(gc.PageReqType_BUILTINJS, "https://medium.com/topic/technology", "infiniteScrollToEnd", "")
	go func() {
		for {
			ph := <-z.PageChan
			fmt.Println(ph.Success, ph.Httpstatuscode, ph.Url, ph.MetaStr)
			// fmt.Println(string(ph.Content))
			err := ioutil.WriteFile("/tmp/out.file", ph.Content, 0775)
			if err != nil {
				fmt.Println("Write failed:", err, "for url:", ph.Url)
			}
		}
	}()

	for {
		if z.IsAlive() {
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}
}
