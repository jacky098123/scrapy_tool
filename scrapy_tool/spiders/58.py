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
    "host"  : "192.168.2.11",
    "user"  : "product_w",
    "passwd": "kooxoo",
    "database"  : "test",
    "port"      : 3306,
    "charset"   : "utf8"
}

class Zz6Spider(BaseSpider, CommonHandler):
    name = "58"
    allowed_domains = ["58.com"]

    iterator = False

    def __init__(self, kxdebug=None):
        self.kxdebug    = kxdebug

        self.db_conn = MySQLOperator()
        if not self.db_conn.Connect(**LOCAL_DB):
            raise Exception, str(**LOCAL_DB)

    def _gen_request(self, url, id):
        request = Request(
            url     = self.ToString(url),
            meta    = {'kx_args': {'retry_times': 0, 'url': url, 'id': id}})
        return request

    def start_requests(self):
#        return [self._gen_request('http://www.58.com/changecity.aspx', 0),] # homepage
        request_list    = []
        sql = "select id,url from seo_landmark.58_quyu where ext_level in (2) and flag = '' and anchor_text not like '%周边' "
#        sql = "select id,url from seo_landmark.58_quyu where id = 3715 "
        result_set = self.db_conn.QueryDict(sql)
        for row in result_set:
            request = self._gen_request(row['url'], row['id'])
            request_list.append(request)
        self.log("total request: %d" % len(request_list))
        pdb.set_trace()
        return request_list

    def parse_parent(self, response):
        hxs = HtmlXPathSelector(response)
        taga_lst = hxs.select(".//div[@class='topcity']//a")
        for taga in taga_lst:
            title = taga.select("text()").extract()
            if len(title) > 0:
                title = title[0]
            else:
                title = ''
            url = taga.select("@href").extract()
            if len(url) > 0:
                url = url[0]
            else:
                url = ''
            if url.find(".58.com") >= 0:
                self.db_conn.Execute("insert ignore into 58_quyu(anchor_text,url) values(%s,%s)", [title,url])

    def _get_single_text(self, _selector, _path):
        l = _selector.select(_path).extract()
        if len(l) > 0:
            return l[0]
        return ''

    def parse(self, response):
# the first page
#        self.parse_parent(response)
#        return

        self.log("-- parse --")
        self.log("content len: %d" % len(response.body))

        kx_args = response.meta['kx_args']
        domain_o = urlparse(kx_args['url'])

        hxs = HtmlXPathSelector(response)
        quyu_selectors = hxs.select(".//div[@class='relative']/dl[@id='filter_quyu']/dd/a")
        quyu_selectors = [] # for landmark only
        sub_selectors = hxs.select(".//div[@class='relative']/dl[@id='filter_quyu']/dd/div[@class='subarea']/a")
        for quyu in quyu_selectors + sub_selectors:
            title = self._get_single_text(quyu, 'text()')
            url = self._get_single_text(quyu, '@href')
            url = url.lower()
#            print "p1", title, url
            if len(title) == 0 or len(url) == 0:
                continue
            if not url.startswith("http:"):
                url = "http://" + domain_o.netloc + url

            # insert into 58_quyu
            sql = "select count(*) from seo_landmark.58_quyu where url = %s"
            cnt = self.db_conn.Query(sql, [url,])[0][0]
            if cnt > 0:
                continue

#            print "p2", title, url
            sql = "insert into seo_landmark.58_quyu(anchor_text, url, pid) values(%s,%s,%s)"
            self.db_conn.Execute(sql, [title, url, kx_args['id']])

            # gen request
            if not self.iterator:
                continue

            sql = "select id from 58_quyu where url = %s"
            res = self.db_conn.Query(sql, [url,])
            if len(res) == 0:
                continue
            _id = res[0][0]
            yield self._gen_request(url, _id)

        area = self._get_single_text(hxs, ".//div[@class='relative']/dl[@id='filter_quyu']")
        subway = self._get_single_text(hxs, ".//*[starts-with(@class,'subwayline')]")
        district = self._get_single_text(hxs, ".//div[@class='tosq']")
        sql = "update seo_landmark.58_quyu set subway=%s,area=%s,district=%s,flag='succ' where id=%s"
        self.db_conn.Execute(sql, [subway, area, district, kx_args['id']])
