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

class Zz6Parse(CommonHandler):
    TEST_FILE = '.zz6.html'

    CONVERT_DICT = {
        u'简称': 'short_name',
        u'面积': 'area',
        u'人口密度': 'population_dencity',
        u'车牌号码': 'license_prefix',
        u'行政级别': 'admin_level',
        u'人口': 'population',
        u'长途区号': 'tele_prefix',
        u'行政代码': 'admin_code',
        u'辖区面积': 'area',
        u'邮政编码': 'post_code',
        u'身份证前六位': 'idcard_prefix',
    }

    def _format(self, item_list, join_flag=False):
        if len(item_list) > 0:
            if join_flag:
                s = u''.join(item_list)
                return s.strip()
            else:
                return item_list[0].strip()
        else:
            return ''

    def parse_descendant(self, html):
        html = self.ToUnicode(html, 'gb2312')
        html = self.ToString(html)
        hxs = HtmlXPathSelector(text=html)
        # get descendant
        data_info_list = []
        descendant_list = hxs.select(".//table[@bgcolor='cccccc' and @width='738']/tr")
        ishead = True
        for descendant_selector in descendant_list:
            if ishead:
                ishead = False
                continue

            td_selector_list = descendant_selector.select(".//td")

            data_info = {}
            descendant_name = td_selector_list[0].select('.//strong/a/text()').extract()
            data_info['descendant_name'] = self._format(descendant_name)
            descendant_url = td_selector_list[0].select('.//strong/a/@tppabs').extract()
            data_info['descendant_url'] = self._format(descendant_url)
            data_info_list.append(data_info)

        for data_dict in data_info_list:
            for k,v in data_dict.iteritems():
                print k, v
        return data_info_list

    def parse_province_info(self, html):
        html = self.ToUnicode(html, 'gb2312')
        html = self.ToString(html)
        hxs = HtmlXPathSelector(text=html)

        data_info = {}
        h1 = hxs.select(".//h1/text()").extract()
        data_info['title'] = self._format(h1)

        info_selector_list = hxs.select(".//div[@id='pagebody']/div[@id='page_left']/table[2]/tr/td[1]/table/tr/td")
        for info_selector in info_selector_list:
            key = info_selector.select('.//strong/text()').extract()
            key = self._format(key)
            value = info_selector.select('text()').extract()
            value = self._format(value)
            value = value.strip(u':')
            if not value:
                value = info_selector.select('.//a/text()').extract()
                value = self._format(value)
            if key and value:
                if self.CONVERT_DICT.get(key):
                    data_info[self.CONVERT_DICT[key]] = value
                elif key.find(u'地址') > 0:
                    data_info['address'] = value
                else:
                    pass
#data_info[key] = value

        desc_list = hxs.select(".//div[@id='pagebody']/div[@id='page_left']/table[2]/tr/td[2]/text()")
        data_info['description'] = ''
        for desc in desc_list:
            data_info['description'] += desc.extract()

        for k,v in data_info.iteritems():
            print k,v
        return data_info

    def parse_sub_info(self, html):
        html = self.ToUnicode(html, 'gb2312')
        html = self.ToString(html)
        hxs = HtmlXPathSelector(text=html)

        data_info = {}
        h1 = hxs.select(".//h1/text()").extract()
        data_info['title'] = self._format(h1)

        info_selector_list = hxs.select(".//table[@width='432px' and @bgcolor='cccccc']/tr/td")
        if len(info_selector_list) == 0:
            info_selector_list = hxs.select(".//table[@width='396px' and @bgcolor='cccccc']/tr/td")
        for info_selector in info_selector_list:
            key = info_selector.select(".//strong/text()").extract()
            key = self._format(key)
            v = info_selector.select("text()").extract()
            v = self._format(v, True)
            if v:
                v = v.strip(":")
            if key and v:
                if self.CONVERT_DICT.get(key):
                    data_info[self.CONVERT_DICT[key]] = v
                else:
                    pass
#                    data_info[key] = v
        f14_desc_list = hxs.select(".//div[@class='f14']/text()").extract()
        data_info['description'] = "\n".join(f14_desc_list)
    

        for k,v in data_info.iteritems():
            print k,v
        return data_info

    def test_parse(self, p=False):
        html_data = self.LoadFile(self.TEST_FILE)
        data_list   = self.parse_province_info(html_data)

    def fetch_file(self):
        url = "http://www.zz6.cn/beijing/"
#        url = "http://www.zz6.cn/chongqing/zhongxian.html"
#        url = "http://www.zz6.cn/hubei/chibi_huanggaihunongchangdi.html"
        print url
        urllib.urlretrieve(url, self.TEST_FILE)

    def test(self):
        self.fetch_file()
        self.test_parse(True)

if __name__ == '__main__':
    z = Zz6Parse()
    z.test()
