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

def extract_boundary(html, former, latter, pos=0, strip_flag=False):
    logging.debug("html len: %d, %s, %s, pos: %d, %s" % (len(html), former, latter, pos, str(strip_flag)))
    if former is None or len(former) == 0:
        former = ''
        pos = 0
    else:
        pos = html.find(former, pos)
    if pos < 0:
        return '', -1
    if latter is None or len(latter) == 0:
        latter = ''
        new_pos = len(html)
    else:
        new_pos = html.find(latter, pos+len(former))
    if new_pos < 0:
        return '', -1
    new_pos = new_pos + len(latter)
    if strip_flag:
        return html[pos+len(former):new_pos-len(latter)], new_pos
    return html[pos:new_pos], new_pos


class BaikeParser(CommonHandler):
    TEST_FILE = '.baike.html'

    def strip_tags(self, html_content):
        html_content = re.sub(r"(?i)<script[^>]*?>.*?</script>", "", html_content)
        html_content = re.sub(r"<[^>]*?>", "", html_content)
        pos = html_content.find(">")
        if pos >= 0:
            html_content = html_content[pos+1:]
        pos = html_content.find("<")
        if pos > 0:
            html_content = html_content[:pos]
        return html_content

    def parse(self, html):
        hxs = HtmlXPathSelector(text=html)

        abstract = hxs.select(".//div[@id='unifyprompt']/div/p").extract()
        if abstract and len(abstract) > 0: 
            abstract = abstract[0]
            if abstract.find(u"添加摘要") < 0:
                abstract = self.strip_tags(abstract)
                return abstract

        content = hxs.select(".//div[@id='content']/p[1]").extract()
        if content and len(content) > 0:
            content = content[0]
            content = self.strip_tags(content)
            if len(content) > 100:
                return content

        content = hxs.select(".//div[@id='content']").extract()
        if content and len(content) > 0:
            content = content[0]
            content, b = extract_boundary(content, "class=\"content_h2 bac_no\">", "<div class=\"content_h2\">", 0, True)
            content = self.strip_tags(content)
            return content

        return ''

    def test(self):
        url = "http://www.baike.com/wiki/%E4%B8%8A%E6%B5%B7%E6%A4%8D%E7%89%A9%E5%9B%AD"
        url = "http://www.baike.com/wiki/%E5%AE%8B%E5%9B%AD%E8%B7%AF%E7%AB%99"
        url = "http://www.baike.com/wiki/%E5%87%A4%E5%87%B0%E6%96%B0%E6%9D%91"
        url = "http://www.baike.com/wiki/%E6%B6%9E%E6%B0%B4"
        print url
        urllib.urlretrieve(url, self.TEST_FILE)
        html_data   = self.LoadFile(self.TEST_FILE)
        data   = self.parse(html_data)
        print data

if __name__ == '__main__':
    z = BaikeParser()
    z.test()
