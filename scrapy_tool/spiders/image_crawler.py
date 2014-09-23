# coding: utf8

import os
import sys
import logging
import urllib
import hashlib
from urlparse import urlparse
from PIL import Image
import pdb

sys.path.append("/home/yangrq/projects/pycore")

from db.mysqlv6 import MySQLOperator
from utils.common_handler import CommonHandler

from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.spider import BaseSpider
from scrapy_tool.items import ScrapyToolItem



LOCAL_DB = {
    "host"  : "192.168.2.11",
#    "host"  : "127.0.0.1",
    "user"  : "product_w",
    "passwd": "kooxoo",
    "database"  : "test",
    "port"      : 3306,
    "charset"   : "utf8"
}

class ImageSpider(BaseSpider, CommonHandler):
    name = "image"
    allowed_domains = ["techan.com"]

    def __init__(self, kxdebug=None):
        self.kxdebug    = kxdebug

        self.db_conn = MySQLOperator()
        if not self.db_conn.Connect(**LOCAL_DB):
            raise Exception, str(**LOCAL_DB)

    def start_requests(self):
        request_list    = []
        sql = "select a.id,a.tc_img from basic_data.techan a left join basic_data.image_info b  "
        sql += " on a.tc_img = b.url where b.url is null"
        self.log(sql)
        result_set = self.db_conn.QueryDict(sql)
        self.log("result_set: %d" % len(result_set))
        for row in result_set:
            request = Request(
                url     = row['tc_img'],
                meta    = {'kx_args': {'retry_times': 0, 'id': row['id'], 'url': row['tc_img']}})
            request_list.append(request)
        self.log("total request: %d" % len(request_list))
        return request_list

    def get_md5(self,content):
        m = hashlib.md5(content)
        return m.hexdigest()

    def parse(self, response):
        self.log("-- parse --")
        self.log("content len: %d" % len(response.body))
        kx_args = response.meta['kx_args']

        image_content = response.body

        if len(image_content) > 20:
            file_name = "/tmp/%s" % self.get_md5(image_content)
            self.log("file_name: %s" % file_name)
            self.SaveFile(file_name, image_content)
            try:
                im = Image.open(file_name)
                db_dict = {}
                db_dict['url']          = kx_args['url']
                db_dict['image_content']= image_content
                db_dict['category']     = 'techan'
                self.db_conn.ExecuteInsertDict('basic_data.image_info', db_dict)
            except Exception, e:
                self.log("invalid Image: %s" % str(e))
                return
            finally:
                os.remove(file_name)
                pass