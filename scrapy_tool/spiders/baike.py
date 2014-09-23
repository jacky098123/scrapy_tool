# coding: utf8

import sys
import logging
import urllib
from urlparse import urlparse
import pdb

sys.path.append("/home/yangrq/projects/pycore")

from db.mysqlv6 import MySQLOperator
from utils.common_handler import CommonHandler

from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.spider import BaseSpider

from scrapy_tool.items import ScrapyToolItem

from parser_baike import BaikeParser

LOCAL_DB = {
    "host"  : "192.168.2.11",
    "user"  : "product_w",
    "passwd": "kooxoo",
    "database"  : "test",
    "port"      : 3306,
    "charset"   : "utf8"
}

class Zz6Spider(BaseSpider, CommonHandler):
    name = "baike"
    allowed_domains = ["baike.com"]

    def __init__(self, kxdebug=None):
        self.kxdebug    = kxdebug

        self.db_conn = MySQLOperator()
        if not self.db_conn.Connect(**LOCAL_DB):
            raise Exception, str(**LOCAL_DB)

    def start_requests(self):
        request_list    = []
        sql = "select id,name from test.yang_landmark_poi where flag = 'init3' limit 500000 "
        result_set = self.db_conn.QueryDict(sql)
        for row in result_set:
            request = Request(
                url     = "http://www.baike.com/wiki/" + urllib.quote(self.ToString(row['name'])),
                meta    = {'kx_args': {'retry_times': 0, 'id': row['id']}})
            request_list.append(request)
        self.log("total request: %d" % len(request_list))
        return request_list


    def parse(self, response):
        self.log("-- parse --")
        self.log("content len: %d" % len(response.body))

        kx_args = response.meta['kx_args']

        baike_parser = BaikeParser()
        content = baike_parser.parse(response.body)
        if len(content) > 10:
            sql = "update test.yang_landmark_poi set description = %s, flag = 'succ' where id = %s "
            self.db_conn.Execute(sql, [content, kx_args['id']])
        else:
            sql = "update test.yang_landmark_poi set flag = 'failed' where id = %s"
            self.db_conn.Execute(sql, [kx_args['id'],])
