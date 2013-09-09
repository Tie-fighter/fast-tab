#!/usr/bin/env python
# This file is part of fast-tab and licensed under The MIT License (MIT).

import threading
import re
import time

import traceback

from config import *

# TODO rewrite
# TODO start here ->

class ProcessorThread(threading.Thread):
    def __init__(self, logger, processing_queue, processed_dict):
        threading.Thread.__init__(self)
        self.logger = logger
        self.processing_queue = processing_queue
        self.processed_dict = processed_dict

        self.config = Config()
        self.config.read_config()

        # connect to database
        self.db_conn = psycopg2.connect(host = self.config.db_host, user = self.config.db_user, password = self.config.db_password, database = self.config.db_database)
        self.db_cursor = self.db_conn.cursor()

    def __del__(self):
        self.logger.info("processor died")
        self.db_conn.close()

    def run(self):
        self.logger.debug("processor spawned")
        while True:
            item = self.processing_queue.get()
            identifier = item[0]
            url = item[1]
            html = item[2]
            self.logger.info("processing: " + identifier)

            name = self.extract_name(html).replace("\\", "\\\\")
            developer = self.extract_developer(html).replace("\\", "\\\\")
            rating = self.extract_rating(html)
            rating_count = self.extract_rating_count(html)
            update_date = self.extract_update_date(html)
            version = self.extract_version(html)
            category = self.extract_category(html)
            download = self.extract_download(html)
            size = self.extract_size(html)
            price = self.extract_price(html)
            content_rating = self.extract_content_rating(html)
            permissions = self.extract_permissions(html)
            
            self.logger.debug("extracted: " +name+ " " +developer+ " " +rating+ " " +rating_count+ " " +update_date+ " " +version+ " " +category+ " " +download+ " " +size+ " " +price+ " " +content_rating+ " " +str(permissions))

            self.update_database(identifier, name, developer, rating, rating_count, update_date, version, category, download, size, price, content_rating, permissions)
            self.processed_dict[identifier] = [ url, "processed" ]

            self.processing_queue.task_done()


    def fetch_page(self, url):
        f = urllib.urlopen(url)
        return ''.join(f.readlines())

    def extract_name(self, html):
        #<h1 class="doc-banner-title">Dream Heights</h1>
        match = re.findall('class="doc-banner-title">(.*?)<\/h1>', html)
        if match:
            return '\'' + match[0] + '\''
        else:
            return 'NULL'

    def extract_developer(self, html):
        #<a href="/store/apps/developer?id=Zynga" class="doc-header-link">Zynga</a>
        match = re.findall('class="doc-header-link">(.*?)</a>', html)
        if match:
            return '\'' + match[0] + '\''
        else:
            return 'NULL'
        
    def extract_rating(self, html):
        #<div class="ratings goog-inline-block" title="Rating: 4.8 stars (Above average)" itemprop="ratingValue" content="4.8">
        match = re.findall('itemprop="ratingValue" content="([0-9,.]*?)"', html)
        if match:
            return '\'' + match[0] + '\''
        else:
            return 'NULL'

    def extract_rating_count(self,html):
        #<span itemprop="ratingCount" content="3402">3,402</span>
        match = re.findall('itemprop="ratingCount" content="([0-9]*?)"', html)
        if match:
            return '\'' + match[0] + '\''
        else:
            return '\'0\''

    def extract_update_date(self, html):
        #<time itemprop="datePublished">March 30, 2012</time>
        match = re.findall('itemprop="datePublished">(.*?)<', html)
        if match:
            return '\'' + match[0] + '\''
        else:
            return 'NULL'

    def extract_version(self, html):
        #<dd itemprop="softwareVersion">1.0.1</dd>
        match = re.findall('itemprop="softwareVersion">(.*?)<', html)
        if match:
            return '\'' + match[0] + '\''
        else:
            return 'NULL'

    def extract_category(self, html):
        #<dt>Category:</dt>\w<dd><a href="/store/apps/category/BRAIN?feature=category-nav">Brain &amp; Puzzle</a></dd>
        match = re.findall('<dt>Category:<\/dt>\s?<dd><a href="\/store\/apps\/category\/([A-Z\_]*)', html)
        if match:
            return '\'' + match[0] + '\''
        else:
            return 'NULL'

    def extract_download(self, html):
        #<dd itemprop="numDownloads">100,000 - 500,000<div class="normalized-daily-installs-chart" style="width: 105px;">
        match = re.findall('itemprop="numDownloads">(.*?)<', html)
        if match:
            return '\'' + match[0] + '\''
        else:
            return 'NULL'

    def extract_size(self, html):
        #<dd itemprop="fileSize">19M</dd>
        match = re.findall('itemprop="fileSize">(.*?)<', html)
        if match:
            return '\'' + match[0] + '\''
        else:
            return 'NULL'

    def extract_price(self, html):
        #<meta itemprop="price" content="0,76&nbsp;">
        match = re.findall('itemprop="price" content="([0-9,]*).*?"', html)
        if match:
            return '\'' + match[0].replace(',', '.') + '\''
        else:
            return 'NULL'

    def extract_content_rating(self, html):
        #<dd itemprop="contentRating">Low Maturity</dd>
        match = re.findall('itemprop="contentRating">(.*?)<', html)
        if match:
            return '\'' + match[0] + '\''
        else:
            return 'NULL'

    def extract_permissions(self, html):
        #<div class="doc-permission-description">coarse (network-based) location</div>
        match = re.findall('class="doc-permission-description">(.*?)<', html)
        if match:
            return list(match)
        else:
            return ('None',)

    def try_field(self, field_dict, field_name, value):
        if (value != 'NULL'):
            self.db_cursor.execute('SELECT id FROM "public"."'+field_name+'" WHERE value = '+value+'')
            field_dict[field_name] = self.db_cursor.fetchone()
            if (field_dict[field_name] != None):
                field_dict[field_name] = '\'' +str(int(field_dict[field_name][0]))+ '\''
            else:
                self.logger.debug(field_name+" not found in Database: " + value + " -> inserting")
                print field_name, field_dict[field_name]
                self.db_cursor.execute('INSERT INTO "public"."'+field_name+'" (value) VALUES ('+value+')')
                self.db_cursor.execute('SELECT id FROM "public"."'+field_name+'" WHERE value = '+value+'')
                field_dict[field_name] = '\'' +str(int(self.db_cursor.fetchone()[0]))+ '\''
        else:
            field_dict[field_name] = 'NULL'

    def update_database(self, identifier, name, developer, rating, rating_count, update_date, version, category, download, size, price, content_rating, permissions):

#        print identifier, name, developer, rating, rating_count, update_date, version, category, download, size, price, content_rating, permissions

        perms = ""
        perm_dict = dict()
        for perm in permissions:
            perm_dict[perm] = ''
        for perm in perm_dict:
            perms = perms + perm + ', '
        perms = perms.rstrip(' ,')

        field_dict = {}

        # 
        # try all the values and insert if necessary
        
        try:
            # developers
            self.try_field(field_dict, "developers", developer)

            # categories
            self.try_field(field_dict, "categories", category)

            # icons
            # TODO

            # ratings
            self.try_field(field_dict, "ratings", rating)

            # rating_count
            self.try_field(field_dict, "rating_counts", rating_count)

            # downloads
            self.try_field(field_dict, "downloads", download)

            # size
            self.try_field(field_dict, "sizes", size)

            # price
            self.try_field(field_dict, "prices", price)

            # update dates
            self.try_field(field_dict, "update_dates", update_date)

            # versions
            self.try_field(field_dict, "versions", version)

            # content_rating
            self.try_field(field_dict, "content_ratings", content_rating)

            now = str(time.time())

            # application
            self.db_cursor.execute('UPDATE "public"."applications" SET last_time_processed = to_timestamp(\''+str(now)+'\'), name = '+name+' WHERE identifier = \''+identifier+'\';')
            self.db_cursor.execute('SELECT id FROM "public"."applications" WHERE identifier = \''+identifier+'\';')
            field_dict["applications"] = '\'' +str(int(self.db_cursor.fetchone()[0]))+ '\''

            # point in time
#            print ('INSERT INTO "public"."pointsintime" (timestamp, application_id, developer_id, category_id, rating_id, rating_count_id, download_id, size_id, price_id, update_date_id, version_id, content_rating_id) VALUES ( to_timestamp(\''+str(now)+'\'), '+field_dict["applications"]+', '+field_dict["developers"]+', '+field_dict["categories"]+', '+field_dict["ratings"]+', '+field_dict["rating_counts"]+', '+field_dict["downloads"]+', '+field_dict["applications"]+', '+field_dict["prices"]+', '+field_dict["update_dates"]+', '+field_dict["versions"]+', '+field_dict["content_ratings"]+')')

            self.db_cursor.execute('INSERT INTO "public"."pointsintime" (timestamp, application_id, developer_id, category_id, rating_id, rating_count_id, download_id, size_id, price_id, update_date_id, version_id, content_rating_id) VALUES ( to_timestamp(\''+str(now)+'\'), '+field_dict["applications"]+', '+field_dict["developers"]+', '+field_dict["categories"]+', '+field_dict["ratings"]+', '+field_dict["rating_counts"]+', '+field_dict["downloads"]+', '+field_dict["sizes"]+', '+field_dict["prices"]+', '+field_dict["update_dates"]+', '+field_dict["versions"]+', '+field_dict["content_ratings"]+')')
            self.db_cursor.execute('SELECT id FROM "public"."pointsintime" WHERE timestamp = to_timestamp('+str(now)+') AND application_id = '+field_dict["applications"]+'')
            field_dict["pointsintime"] = '\'' +str(int(self.db_cursor.fetchone()[0]))+ '\''

            # permissions
            for perm in perm_dict:
                self.db_cursor.execute('SELECT id FROM "public"."permissions" WHERE regex = \''+perm+'\'')
                perm_id = self.db_cursor.fetchone()
                if (perm_id != None):
                    perm_id = int(perm_id[0])
                else:
                    self.logger.debug("permission not found in Database: " +perm+ " -> inserting")
                    self.db_cursor.execute('INSERT INTO "public"."permissions" (name, description, regex) VALUES ( \'unknown\', Null, \''+perm+'\' )');
                    self.db_cursor.execute('SELECT id FROM "public"."permissions" WHERE regex = \''+perm+'\'')
                    perm_id = int(self.db_cursor.fetchone()[0])
                self.db_cursor.execute('INSERT INTO "public"."pointintime_permissions" (pointintime_id, permission_id) VALUES ( '+field_dict["pointsintime"]+', '+str(perm_id)+' );')


            self.db_conn.commit()
        except Exception, err:
            self.logger.error("Exception - " + str(err))
            self.db_conn.rollback()
