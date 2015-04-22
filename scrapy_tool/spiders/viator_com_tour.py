# coding: utf8

import sys
import logging
import urllib
import re
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
    name = "viator_tour"
    allowed_domains = ["viator.com"]

    iterator = False

    def __init__(self, kxdebug=None):
        self.kxdebug    = kxdebug

        self.db_conn = MySQLOperator()
        if not self.db_conn.Connect(**LOCAL_DB):
            raise Exception, str(**LOCAL_DB)

    def _gen_request(self, url, id, first_page=0):
        print 'URL', url
#        url     = self.ToString(url).strip('/')
#        items   = url.split("/")
#        items[-2] = items[-2] + "-tours-tickets"
        url     = "http://www.viator.com" + self.ToString(url)
        request = Request(
            url     = url,
            meta    = {'kx_args': {'retry_times': 0, 'url': url, 'id': id, 'first_page': first_page}})
        return request

    def start_requests(self):
        request_list    = []
        sql = "select id,title,href from viator_destination_attraction_unique where flag='init' "
        result_set = self.db_conn.QueryDict(sql)
        for row in result_set:
            request = self._gen_request(row['href'], row['id'], 1)
            request_list.append(request)
        self.log("total request: %d" % len(request_list))
        return request_list

    def parse(self, response):
        hxs = HtmlXPathSelector(response)
        kx_args = response.meta.get('kx_args')

        tour_selector_lst = hxs.select(".//div[@class='media product-summary mhn mtn']")
        for _selector in tour_selector_lst:
            href = self._get_single_text(_selector, ".//div[@class='img product-image mrl']/a/@href")
            title = self._get_single_text(_selector, ".//h2/a/text()")
            description = self._get_single_text(_selector, ".//p[@class='man mts']/text()")
            location = self._get_single_text(_selector, ".//p[@class='man mts note xsmall']/text()")
            price_lst = _selector.select(".//div[@class='product-price txtR']").extract()
            price   = self.ToString('.'.join(price_lst))
            price   = self._strip_tags(price)
            price   = price.replace("\n", " ").strip()

            price   = ' '.join(price.split())

            sql = "insert into viator_tour(sightid,href,title,description,location,price) values(%s,%s,%s,%s,%s,%s)"
            self.db_conn.Execute(sql, [kx_args['id'],href,title,description,location,price])

        if len(tour_selector_lst) > 0:
            self.db_conn.Execute("update viator_destination_attraction_unique set flag='done' where id=%s", [kx_args['id'],])
        else:
            self.db_conn.Execute("update viator_destination_attraction_unique set flag='passed' where id=%s", [kx_args['id'],])


    def _get_single_text(self, _selector, _path):
        l = _selector.select(_path).extract()
        if len(l) > 0:
            l = l[0].replace("\n", " ")
            l = l.strip()
            return l
        return ''

    def _strip_tags(self, item_content):
        item_content = re.sub(r"(?i)<script[^>]*?>.*?</script>", "", item_content)
        item_content = re.sub(r"<[^>]*?>", "", item_content)
        pos = item_content.find(">")
        if pos >= 0:
            item_content = item_content[pos+1:]
        pos = item_content.find("<")
        if pos > 0:
            item_content = item_content[:pos]
        return item_content
