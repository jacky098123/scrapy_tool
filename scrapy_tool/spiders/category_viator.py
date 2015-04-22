# coding: utf-8

import os
import sys
import time
import urllib
import logging
import pdb
import traceback
import re
import json
import hashlib
from datetime import datetime, timedelta
from optparse import OptionParser

from scrapy.selector import HtmlXPathSelector
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor

sys.path.append('/home/yangrq/projects/pycore')

from db.mysqlv6 import MySQLOperator
from utils.common_handler import CommonHandler
from utils.http_client import HttpClient
from utils.btlog import btlog_init

DB_57 = {
    "host" : "192.168.0.57",
    "port" : 3306,
    "database"  : "viator_crawl",
    "user"      : "product_w",
    "passwd"    : "kooxootest"
}

class ToolTest(CommonHandler, HttpClient):
    RE = re.compile('_(\d{8})')

    def __init__(self):
        self.conn_57           = MySQLOperator()
        if not self.conn_57.Connect(**DB_57):
            raise Exception, "can not connect [%s]" % str(DB_57)


        parser = OptionParser()
        parser.add_option('--full', action='store_true')
        (self.opt, others) = parser.parse_args()

    def _get_taga(self, _selector, _xpath ):
        _lst = []
        taga_selector_lst = _selector.select(_xpath)
        for _selector in taga_selector_lst:
            anchor_text = self._get_single_text(_selector, "text()")
            href        = self._get_single_text(_selector, "@href")
            anchor_text = anchor_text.strip()
            href        = href.strip()
            if not href.startswith("http"):
                href = "http://www.viator.com" + href
                _lst.append([anchor_text, href])

        return _lst

    def get_group(self, hre):
        html_content = urllib.urlopen(hre).read()
        mypath = ".//div[@class='bd']/ul[1]/li/ul/li/a"
        html_content = urllib.urlopen(hre).read()
        hxs = HtmlXPathSelector(text=html_content)

        sql = "select id from viator_group where href=%s"
        res = self.conn_57.Query(sql, [hre,])
        pid = res[0][0]

        tag_lst = self._get_taga(hxs, mypath)
        for taga in tag_lst:
            sql = "insert ignore into viator_group(anchor_text,href,pid) values(%s,%s,%s)"
            taga.append(pid)
            self.conn_57.Execute(sql, taga)

    def test(self):
        sql = "select href from viator_destination_attraction_unique order by rand() limit 1000 "
        res = self.conn_57.Query(sql)
        for row in res:
            href = self.ToString(row[0])
            if not href.startswith("http"):
                href = "http://www.viator.com" + href
            self.test_url(href)

    def test_url(self, href):
        '''
        href = "http://www.viator.com/Barcelona-tours/Day-Trips-and-Excursions/d562-g5"
        href = "http://www.viator.com/China/d13-ttd"
        href = "http://www.viator.com/Tokyo/d334-ttd"
        '''
        html_content = urllib.urlopen(href).read()
        hxs = HtmlXPathSelector(text=html_content)
        taga_lst = self._get_taga(hxs, ".//div[@class='bd']/ul[1]/li/a")
        for taga in taga_lst:
            if not taga[1].endswith("ttd"):
                print taga
                sql = "insert ignore into viator_group(anchor_text,href) values(%s,%s)"
                self.conn_57.Execute(sql, taga)
                self.get_group(taga[1])

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

    def do_format(self):
        sql = "select * from viator_group "
        res = self.conn_57.QueryDict(sql)
        for row in res:
            print row['href']
            ll = re.findall(r'-g(\d+)', row['href'])
            print ll
            _groupid = 'g' + ll[0]

            dgroup = row['href'].split("/")[-2]
            self.conn_57.Execute("update viator_group set dgroupid=%s,dgroup=%s where id=%s", [_groupid, dgroup, row['id']])

            
    def check(self):
        sql = "select * from viator_group"
        res = self.conn_57.QueryDict(sql)
        for row in res:
            href = row['href'].replace("http://www.viator.com/", '')
            items = href.split('/')
            if not items[0].endswith('-tours'):
                print items[0], row['id']

    def check2(self):
        sql = "select * from viator_destination_attraction "
        res = self.conn_57.QueryDict(sql)
        for row in res:
            items = row['href'].split('/')

            destination = items[1].replace('-attractions', '')
            destinationid = 0
            ll = re.findall(r'd(\d+)', items[-1])
            if len(ll) > 0:
                destinationid = ll[0]
            self.conn_57.Execute("update viator_destination_attraction set destination=%s,destinationid=%s where id=%s", [destination,destinationid,row['id']])


    def tmp2(self):
        sql = "select distinct anchor_text,dgroup,dgroupid from viator_group "
        self.group_res = self.conn_57.QueryDict(sql)

        sql = "select destination,destinationid from viator_attraction_city where pid=0 "
        tmp_res = self.conn_57.QueryDict(sql)
        for destination_info in tmp_res:
            for group_info in self.group_res:
                new_url = "http://www.viator.com/%s-tours/%s/%s-%s" % (destination_info['destination'], 
                                group_info['dgroup'], destination_info['destinationid'], group_info['dgroupid'])
                sql = "insert into viator_ttd_group(destination,destinationid,dgroup,dgroupid,href,source) values(%s,%s,%s,%s,%s,'proc_gen')"
                self.conn_57.Execute(sql, [destination_info['destination'],destination_info['destinationid'],
                                            group_info['dgroup'],group_info['dgroupid'],new_url])



'''
select distinct anchor_text,dgroup,href_key from viator_group
'''

if __name__ == '__main__':
    btlog_init('log_tool.log', logfile=False, console=True)
    k = ToolTest()
#    k.test()
#    k.do_format()
#    k.check2()
    k.tmp2()
