#/*************************************************************************
# *
# * Copyright 2018 Ideas2IT Technology Services Private Limited.
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# *
# ***********************************************************************/

import sys
import os
import time
sys.path.append(os.path.relpath("../pyclient"))
from pyclient import *


if __name__ == "__main__":
    def printURL(ph, ccObj):
        print(ph.success, ph.httpstatuscode, ph.error, ph.url, ph.metaStr)
        if ph.success:
            with open(ph.url.replace('/', '').replace(':', '').replace('?', ''), "wb") as text_file:
                text_file.write(ph.content)
                
                
    z = ClientCrawler("127.0.0.1", "2345", "http://books.toscrape.com/catalogue/page-1.html", printURL)
    z.Start()
    while True:
        time.sleep(1)
