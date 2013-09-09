#!/usr/bin/env python
# This file is part of Fast-TAB and licensed under The MIT License (MIT)

import time
import logging
import sqlite3

import CrawlerThread
import ProcessorThread


def print_usage():
  print 'USAGE:'
  # TODO
  print ''

def main():

  project_name = 'default' # str(int(time.time()))

  # Setup logging
  logger = logging.getLogger('myapp')
  hdlr = logging.FileHandler(project_name + '.log')
  formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
  hdlr.setFormatter(formatter)
  logger.addHandler(hdlr)
  logger.setLevel(logging.DEBUG)
  logger.info('Starting up.')

  # Create SQLite Database and import scheme
  logger.debug('Trying to connect to "' + project_name + '.db"')
  conn = sqlite3.connect(project_name +'.db')
  c = conn.cursor()

  c.execute('''CREATE TABLE words (word text, count integer)''')
  conn.commit()
  # TODO probably more

  crawling_queue = Queue.Queue()           # [url]
  crawled_dict = dict()                    # {url: "crawled"}
  processing_queue = Queue.Queue()         # [url, text]
  processed_dict = dict()                  # {url: "processed"}
  word_count = 0                           # number of words processed
  
  # Start Crawlers
  for i in range(1):
    crawler = CrawlerThread.CrawlerThread(logger, crawling_queue, crawled_dict, processing_queue, processed_dict)
    crawler.setDaemon(True)
    crawler.start()

  # Start Processors
  for i in range (1):
    processor = ProcessorThread.ProcessorThread(logger, processing_queue, processed_dict, c, word_count)
    processor.setDaemon(True)
    processor.start()


  # display status
  while (crawling_queue.empty() is False or processing_queue.empty is False):
    print "crawl:", crawling_queue.qsize(), "/ crawled:", len(crawled_dict), "/ process:", processing_queue.qsize(), "/processed:", len(processed_dict), "/ words:", word_count
  # TODO add logger.info output





if __name__ == '__main__':
  main()

