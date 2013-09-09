#!/usr/bin/env python
# This file is part of fast-tab and licensed under The MIT License (MIT).

import threading
import urllib
import re
import time

from config import *

class HttpError(BaseException):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class CrawlerThread(threading.Thread):
    def __init__(self, logger, crawling_queue, crawled_dict, processing_queue, processed_dict):
        threading.Thread.__init__(self)
        self.logger = logger
        self.crawling_queue = crawling_queue
        self.crawled_dict = crawled_dict
        self.processing_queue = processing_queue
        self.processed_dict = processed_dict

       # self.config = Config()
       # self.config.read_config()

    def __del__(self):
        self.logger.info("crawled died")
        self.db_conn.close()

    def run(self):
        self.logger.info("crawler spawned")
        while True:
            url = self.crawling_queue.get()
            self.logger.debug("crawling " + url)
            try:
                html = self.fetch_page(url)
            except HttpError as e:
                self.logger.info("HttpError: Code " + e.value + " at " + url)
  # TODO rewrite for normal URLs
  # TODO hier weitermachen ->
                if (re.match('https:\/\/play.google.com\/store\/apps\/details\?id=[^&"?#<>()]*', url) != None):
                    urls = re.findall('\/store\/apps\/details\?id=([^&"?#<>()]*)', url)[0]
                continue

            # add found urls to queue
            identifiers = self.find_identifiers(html)
            for identifier in identifiers:
                self.logger.debug("found " + identifier + " at " + url)
                if (self.discovered_dict.has_key(identifier) is False):
                    url_app = self.config.app_url + identifier + "&hl=en"
                    self.crawling_queue.put(url_app)
                    self.discovered_dict[identifier] = url_app
                    self.logger.debug("added for visit: " + url_app)

            # check if this page is to be processed
            # TODO: use escaped self.config.app_url
            if (re.match('https:\/\/play.google.com\/store\/apps\/details\?id=[^&"?#<>()]*', url) != None):
                identifier = re.findall('\/store\/apps\/details\?id=([^&"?#<>()]*)', url)[0]
                if (self.discovered_dict.has_key(identifier) is False):
                    self.discovered_dict[identifier] = url
                self.logger.debug("added for processing: " + identifier + " from " + url)
                item = (identifier, url, html)
                self.processing_queue.put(item)

            self.crawled_dict[url] = "crawled"
            self.crawling_queue.task_done()
            time.sleep(1)

    def fetch_page(self, url):
        f = urllib.urlopen(url)

        code = str(f.getcode())

        if (re.findall('(2\\d\\d|3\\d\\d)', code)):
            return ''.join(f.readlines())
        else:
            raise HttpError(str(f.getcode()))

