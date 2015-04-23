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

LOCAL_DB = {
    "host"  : "192.168.0.14",
    "user"  : "seo",
    "passwd": "kooxoo",
    "database"  : "links",
    "port"      : 3306,
    "charset"   : "utf8"
}

class Zz6Spider(BaseSpider, CommonHandler):
    name = "external_link_checker"
    allowed_domains = ["baidu.com"]

    iterator = False

    def __init__(self, kxdebug=None):
        self.kxdebug    = kxdebug

        self.db_conn = MySQLOperator()
        if not self.db_conn.Connect(**LOCAL_DB):
            raise Exception, str(**LOCAL_DB)

    def start_requests(self):
        request_list    = []
        sql = "select id,inboundfrom,inboundto from linkexchanges where inboundfrom=outboundto and inboundto=outboundfrom and inboundto like '%kuxun.cn%' "
#        sql = "select id,inboundfrom,inboundto from linkexchanges where inboundfrom=outboundto and inboundto=outboundfrom and inboundfrom = 'http://www.lietou.com/szhunter.shtml' limit 1"
        result_set = self.db_conn.QueryDict(sql)
        for row in result_set:
            url = self.ToString(row['inboundfrom'])
            if not url.startswith('http://'):
                url = 'http://' + url
            request = Request(url = url, meta={'kx_args': row})
            request_list.append(request)
        self.log("total request: %d" % len(request_list))
        return request_list

    def parse(self, response):
        kx_args = response.meta['kx_args']

        if response.status >= 300 or response.status < 200:
            self.db_conn.Execute("insert ignore into linkexchanges_check_by_yang(linkexchangesid, flag) values(%s,%s)", [kx_args['id'], 'status:%d'%response.status])
            return

        kuxun_url = self.ToString(kx_args['inboundto'])
        flag = 'missed'
        if response.body.find(kuxun_url) >= 0:
            flag = 'matched'
        
        self.db_conn.Execute("insert ignore into linkexchanges_check_by_yang(linkexchangesid, flag) values(%s,%s)", [kx_args['id'], flag])
