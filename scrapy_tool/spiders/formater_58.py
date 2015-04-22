#!/usr/bin/python
# coding: utf-8

import os
import sys
import time
import urllib
import logging
import re
import pdb
import threading
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from optparse import OptionParser
from urlparse import urlparse

from scrapy.selector import HtmlXPathSelector
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor

FILE_PATH = os.path.realpath(os.path.dirname(__file__))

CORE_PATH = '/home/yangrq/projects/pycore'
if CORE_PATH not in sys.path:
    sys.path.append(CORE_PATH)

from db.mysqlv6 import MySQLOperator
from utils.http_client import HttpClient
from utils.common_handler import CommonHandler
from utils.btlog import btlog_init

LOCAL_DB = {
    "host"  : "192.168.2.11",
    "user"  : "product_w",
    "passwd": "kooxoo",
    "database"  : "test",
    "port"      : 3306,
    "charset"   : "utf8"
}

class Zz6Formater(CommonHandler):
    def __init__(self, **kwargs):
        parser = OptionParser()
        parser.add_option('--process_date', help='date format 0000-00-00', default=datetime.now().strftime('%Y-%m-%d'))
        parser.add_option('--online', default=False, action='store_true')
        (self.opt, other) = parser.parse_args()

        self.db_conn = MySQLOperator()
        if not self.db_conn.Connect(**LOCAL_DB):
            logging.error("can not connect [%s]" % str(LOCAL_DB))
            raise Exception, "err"

    def calculate_path_level(self):
        city_dict = {}
        sql = "select id,pid from seo_landmark.58_quyu "
        result_set = self.db_conn.QueryDict(sql)
        for row in result_set:
            city_dict[row['id']] = row['pid']

        for k in city_dict.keys():
            city_id = k
            path = str(k)
            k = city_dict[k]
            while k:
                path = str(k) + "|" + path
                k = city_dict[k]
            level = len(path.split('|'))
            sql = "update seo_landmark.58_quyu set ext_path = %s, ext_level=%s  where id = %s"
            self.db_conn.Execute(sql, [path, level, city_id])
            
    '''
    exclude: cn.58.com, diaoyudao.58.com
    '''
    def to_online(self):
        def _get_links(html, source_url):
            print html
            hxs = HtmlXPathSelector(text=html)
            taga_lst = hxs.select(".//a")
            ret_lst = []
            for taga in taga_lst:
                title = taga.select("text()").extract()
                if title:
                    title = title[0]
                else:
                    title = ''
                url = taga.select("@href").extract()
                if url:
                    url = url[0]
                else:
                    url = ''
                if title and url:
                    if not url.startswith('http:') and not url.startswith('/'):
                        url = '/' + url
                    if not url.startswith('http:'):
                        o = urlparse(source_url)
                        url = "http://" + o.netloc + url
                    ret_lst.append([title, url])
            return ret_lst

        sql = "select * from seo_landmark.58_quyu where id = 786"
        res = self.db_conn.QueryDict(sql)
        for row in res:
            for column in ('subway', 'district'):
                if len(row[column]) > 0:
                    taga_lst = _get_links(row[column], row['url'])
                    for taga in taga_lst:
                        print taga


if __name__ == '__main__':
    btlog_init('log_58.log', logfile=False, console=True)
    d = Zz6Formater()
    d.calculate_path_level()
#    d.to_online()
