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

from parser_zz6 import Zz6Parse

LOCAL_DB = {
    "host"  : "127.0.0.1",
    "user"  : "root",
    "passwd": "51manhua",
    "database"  : "test",
    "port"      : 3306,
    "charset"   : "utf8"
}

class Zz6Spider(BaseSpider, CommonHandler):
    name = "zz6"
    allowed_domains = ["zz6.cn"]

    def __init__(self, kxdebug=None):
        self.kxdebug    = kxdebug

        self.db_conn = MySQLOperator()
        if not self.db_conn.Connect(**LOCAL_DB):
            raise Exception, str(**LOCAL_DB)

    def start_requests(self):
        request_list    = []
#        sql = "select * from zz6_info where path like '27|%' "
        sql = "select * from zz6_info "
        result_set = self.db_conn.QueryDict(sql)
        for row in result_set:
            request = Request(
                url     = self.ToString(row['url']),
                meta    = {'kx_args': {'retry_times': 0, 'id': row['id']}})
            request_list.append(request)
        self.log("total request: %d" % len(request_list))
        return request_list

    def parse_descendant(self, response):
        kx_args = response.meta['kx_args']
        hxs = HtmlXPathSelector(response)
        parse = Zz6Parse()
        descendant_list = parse.parse_descendant(response.body)
        for descendant in descendant_list:
            db_dict = {}
            db_dict['anchor_text']  = descendant['descendant_name']
            db_dict['url']          = descendant['descendant_url']
            db_dict['pid']          = kx_args['id']
            self.db_conn.Upsert('zz6_info', db_dict, ['url',])

    def parse_sub_info(self, response):
        kx_args = response.meta['kx_args']
        hxs = HtmlXPathSelector(response)
        parse = Zz6Parse()
        sub_info = parse.parse_sub_info(response.body)
        sub_info['id']          = kx_args['id']
        self.db_conn.Upsert('zz6_info', db_dict, ['id',])

    def parse(self, response):
        self.log("-- parse --")
        self.log("content len: %d" % len(response.body))

        self.parse_sub_info(response)

