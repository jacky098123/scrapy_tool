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

from parser_zz6_new import Zz6ParseNew

LOCAL_DB = {
    "host"  : "192.168.2.11",
    "user"  : "product_w",
    "passwd": "kooxoo",
    "database"  : "basic_data",
    "port"      : 3306,
    "charset"   : "utf8"
}

class Zz6Spider(BaseSpider, CommonHandler):
    name = "zz6"
    allowed_domains = ["zz6.cn"]

    iterator = False

    def __init__(self, kxdebug=None):
        self.kxdebug    = kxdebug

        self.db_conn = MySQLOperator()
        if not self.db_conn.Connect(**LOCAL_DB):
            raise Exception, str(**LOCAL_DB)

    def _gen_request(self, url, pid):
        request = Request(
            url     = self.ToString(url),
            meta    = {'kx_args': {'retry_times': 0, 'url': url, 'pid': pid}})
        return request

    def start_requests(self):
        request_list    = []
        sql = "select * from zz6_info_new where ext_level > 3"
        result_set = self.db_conn.QueryDict(sql)
        for row in result_set:
            request = self._gen_request(row['url'], row['pid'])
            request_list.append(request)
        self.log("total request: %d" % len(request_list))
        return request_list

    def parse(self, response):
        self.log("-- parse --")
        self.log("content len: %d" % len(response.body))

        kx_args = response.meta['kx_args']
        parser = Zz6ParseNew()

        # this is province page
        if kx_args['pid'] == 0:
            data_info = parser.parse_province_info(response.body)
            data_info['url'] = kx_args['url']
            self.db_conn.Upsert('zz6_info_new', data_info, ['url',])
            _id = self.db_conn.Query("select id from zz6_info_new where url = %s", [kx_args['url'],])[0][0]

            descendant_list = parser.parse_descendant(response.body)
            for descendant in descendant_list:
                descendant['pid'] = _id
                print descendant
                self.db_conn.Upsert('zz6_info_new', descendant, ['url',])

            if not self.iterator: #
                return

            for descendant in descendant_list:
                yield self._gen_request(descendant['url'], _id)

        # sub pages
        else:
            data_info = parser.parse_sub_info(response.body)
            data_info['url'] = kx_args['url']
            data_info['pid'] = kx_args['pid']
            self.db_conn.Upsert('zz6_info_new', data_info, ['url',])
            _id = self.db_conn.Query("select id from zz6_info_new where url = %s", [kx_args['url'],])[0][0]

            descendant_list = parser.parse_descendant(response.body)
            for descendant in descendant_list:
                descendant['pid'] = _id
                self.db_conn.Upsert('zz6_info_new', descendant, ['url',])

            if not self.iterator:
                return

            for descendant in descendant_list:
                yield self._gen_request(descendant['url'], _id)
