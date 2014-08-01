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

class IncludeSpider(BaseSpider, CommonHandler):
    name = "zz6"
    allowed_domains = ["zz6.cn"]

    def __init__(self, kxdebug=None):
        self.kxdebug    = kxdebug
        self.prefix     = 'http://www.zz6.cn/'

    def start_requests(self):
        request_list    = []
        url_list = [
            'http://www.zz6.cn/'
            ]
        for url in url_list:
            request = Request(
                url     = url,
                meta    = {'kx_args': {'retry_times': 0, }})
            request_list.append(request)
        self.log("total request: %d" % len(request_list))
        return request_list

    def parse(self, response):
        self.log("-- parse --")
        self.log("content len: %d" % len(response.body))

        html    = response.body
        kx_args = response.meta['kx_args']

#        self.SaveFile("html_include_baidu.html", response.body)
        hxs = HtmlXPathSelector(response)
#        title = hxs.select('.//table/tbody/tr/td/h3/text()').extract()
        title_list = hxs.select('.//table/tbody/tr/td/h3/a/text()').extract()
        item_selectors = hxs.select('.//table/tbody/tr')
        for item_selector in item_selectors:
            self.log('parse table item')
            title_list = item_selector.select('.//td/h3/a/text()').extract()
            if len(title_list) > 0:
                title = title_list[0].strip()
                self.log('*** TITLE: %s ***' % self.ToString(title))
            price_list = item_selector.select('.//td[3]/p/span/span/span/text()').extract()
            if len(price_list) > 0:
                price = price_list[0].strip()
                self.log('*** price: %s ***' % self.ToString(price))
            self.log("")

