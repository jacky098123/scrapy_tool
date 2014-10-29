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

class Zz6ParseNew(CommonHandler):
    TEST_FILE = '.zz6new.html'

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

    def get_single_item(self, _selector, _path):
        _lst = _selector.select(_path).extract()
        if len(_lst) == 0:
            return u''
        return _lst[0].strip()

    def get_multiple_items(self, _selector, _path):
        _lst = _selector.select(_path).extract()
        return _lst

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
            data_info['anchor_text'] = self.get_single_item(td_selector_list[0], './/strong/a/text()')
            data_info['url'] = self.get_single_item(td_selector_list[0], './/strong/a/@tppabs')
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
        data_info['title'] = self.get_single_item(hxs, ".//h1/text()")

        info_selector_list = hxs.select(".//*[@id='page_left']/table[2]/tr/td[1]/table/tr/td")
        for info_selector in info_selector_list:
            key     = self.get_single_item(info_selector, './/strong/text()')
            value   = self.get_single_item(info_selector, 'text()')
            value   = value.strip(u':')
            if not value:
                value = self.get_single_item(info_selector, './/a/text()')
            if key and value:
                if self.CONVERT_DICT.get(key):
                    data_info[self.CONVERT_DICT[key]] = value
                elif key.find(u'地址') > 0:
                    data_info['address'] = value
                else:
                    logging.warn("key: %s, value: %s" % (self.ToString(key), self.ToString(value)))

        _lst = self.get_multiple_items(hxs, ".//*[@id='page_left']/table[2]/tr/td[2]/text()")
        data_info['description'] = "\n".join(_lst)


        for k,v in data_info.iteritems():
            print k,v
        return data_info

    def parse_sub_info(self, html):
        html = self.ToUnicode(html, 'gb2312')
        html = self.ToString(html)
        hxs = HtmlXPathSelector(text=html)

        data_info = {}
        data_info['title'] = self.get_single_item(hxs, ".//h1/text()")

        info_selector_list = hxs.select(".//table[@width='432px' and @bgcolor='cccccc']/tr/td")
        if len(info_selector_list) == 0:
            info_selector_list = hxs.select(".//table[@width='396px' and @bgcolor='cccccc']/tr/td")
        for info_selector in info_selector_list:
            key = self.get_single_item(info_selector, ".//strong/text()")
            v = self.get_multiple_items(info_selector, "text()")
            v = "\n".join(v)
            v = v.strip()
            if v:
                v = v.strip(":")
            if key and v:
                if self.CONVERT_DICT.get(key):
                    data_info[self.CONVERT_DICT[key]] = v
                else:
                    logging.warn("key: %s, value: %s" % (self.ToString(key), self.ToString(v)))

        f14_desc_list = hxs.select(".//div[@class='f14']/text()").extract()
        data_info['description'] = "\n".join(f14_desc_list)
    
        f12_lst = self.get_multiple_items(hxs, ".//*[@id='page_left']//div[@class='f12']/text()")
        data_info['f12'] = "\n".join(f12_lst)

        for k,v in data_info.iteritems():
            print k,v
        return data_info

    def test(self):
        url = "http://www.zz6.cn/hebei/xinshiqu_xianfengjiedao.html"
        url = "http://www.zz6.cn/hebei/xinshiqu.html"
#        url = "http://www.zz6.cn/hubei/chibi_huanggaihunongchangdi.html"
        print url
        urllib.urlretrieve(url, self.TEST_FILE)
        html_data = self.LoadFile(self.TEST_FILE)
#        data_dict   = self.parse_province_info(html_data)
        data_dict = self.parse_sub_info(html_data)
#        self.parse_descendant(html_data)

if __name__ == '__main__':
    z = Zz6ParseNew()
    z.test()
