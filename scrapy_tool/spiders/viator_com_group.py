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
    "host"  : "192.168.0.57",
    "user"  : "product_w",
    "passwd": "kooxootest",
    "database"  : "viator_crawl",
    "port"      : 3306,
    "charset"   : "utf8"
}

class Zz6Spider(BaseSpider, CommonHandler):
    name = "viator_group"
    allowed_domains = ["viator.com"]

    iterator = False

    def __init__(self, kxdebug=None):
        self.kxdebug    = kxdebug

        self.db_conn = MySQLOperator()
        if not self.db_conn.Connect(**LOCAL_DB):
            raise Exception, str(**LOCAL_DB)

        sql = "select "

    def _gen_request(self, url, id, first_page=0):
        print 'URL', url
        request = Request(
            url     = self.ToString(url),
            meta    = {'kx_args': {'retry_times': 0, 'url': url, 'id': id, 'first_page': first_page}})
        return request

    def start_requests(self):
        request_list    = []
        sql = "select id,anchor_text,href from viator_attraction_city where flag='init' "
#        sql = "select id,anchor_text,href from viator_attraction_city where id=4 "
        result_set = self.db_conn.QueryDict(sql)
        for row in result_set:
            request = self._gen_request(row['href'], row['id'], 1)
            request_list.append(request)
        self.log("total request: %d" % len(request_list))
        return request_list

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        kx_args = response.meta.get('kx_args')

        attraction_selector_lst = hxs.select(".//div[@class='unit size1of3']//div[@class='ttd-row-height']")
        if len(attraction_selector_lst) > 0:
            for _selector in attraction_selector_lst:
                href = self._get_single_text(_selector, ".//div[@class='product-image']/a/@href")
                title = self._get_single_text(_selector, ".//h2/a/text()")
                description = self._get_single_text(_selector, ".//p[@class='man mts']/text()")
                sql = "insert into viator_destination_attraction(pid,title,href,description) values(%s,%s,%s,%s)"
                self.db_conn.Execute(sql, [kx_args['id'],title,href,description])
        else:
            attraction_selector_lst = hxs.select(".//div[@class='media product-summary mtn mhn']")
            for _selector in attraction_selector_lst:
                href = self._get_single_text(_selector, ".//div[@class='img mrl prs']/a/@href")
                title = self._get_single_text(_selector, ".//h2/a/text()")
                description = self._get_single_text(_selector, ".//p[@class='man mts']/text()")

                sql = "insert into viator_destination_attraction(pid,title,href,description) values(%s,%s,%s,%s)"
                self.db_conn.Execute(sql, [kx_args['id'],title,href,description])

        # subpages
        subpage_selector_lst = hxs.select(".//p[@class='txtR mhn mbl']")
        if len(subpage_selector_lst) > 0 and kx_args['first_page'] == 1:
            page_lst = subpage_selector_lst[0].select(".//a/@href").extract()
            for page in page_lst:
                url = "http://www.viator.com" + self.ToString(page)
                yield self._gen_request(url, kx_args['id'])

        if len(attraction_selector_lst) > 0:
            self.db_conn.Execute("update viator_attraction_city set flag='done' where id=%s", [kx_args['id'],])

    def _get_single_text(self, _selector, _path):
        l = _selector.select(_path).extract()
        if len(l) > 0:
            l = l[0].replace("\n", " ")
            l = l.strip()
            return l
        return ''
