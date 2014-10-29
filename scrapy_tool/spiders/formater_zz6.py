#!/usr/bin/python
# coding: utf-8

import os
import sys
import time
import urllib
import logging
import re
import pdb
import threading
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from optparse import OptionParser

FILE_PATH = os.path.realpath(os.path.dirname(__file__))

CORE_PATH = '/home/yangrq/projects/pycore'
if CORE_PATH not in sys.path:
    sys.path.append(CORE_PATH)

from db.mysqlv6 import MySQLOperator
from utils.http_client import HttpClient
from utils.common_handler import CommonHandler
from utils.btlog import btlog_init

LOCAL_DB = {
    "host"  : "192.168.2.11",
    "user"  : "product_w",
    "passwd": "kooxoo",
    "database"  : "basic_data",
    "port"      : 3306,
    "charset"   : "utf8"
}

class Zz6Formater(CommonHandler):
    def __init__(self, **kwargs):
        parser = OptionParser()
        parser.add_option('--process_date', help='date format 0000-00-00', default=datetime.now().strftime('%Y-%m-%d'))
        parser.add_option('--online', default=False, action='store_true')
        (self.opt, other) = parser.parse_args()

        self.db_conn = MySQLOperator()
        if not self.db_conn.Connect(**LOCAL_DB):
            logging.error("can not connect [%s]" % str(LOCAL_DB))
            raise Exception, "err"

    def _stat(self, tmp_root, k):
        d_lst = self.pid_id_dict.get(k, [])
        if len(d_lst) == 0:
            return
        for d in d_lst:
            tmp_root.setdefault(d,{})
            self._stat(tmp_root[d], d)

    def _stat_p(self, tmp_root, prefix):
        level = len(prefix.split('|'))
        sub_count = len(tmp_root)
        self.p_lst.append("%d\t%s\t%d" % (level, prefix, sub_count))
        if sub_count > 0:
            for k in tmp_root.keys():
                self._stat_p(tmp_root[k], prefix + '|' + str(k))

    def stat(self):
        id_pid_dict = {}
        self.pid_id_dict = {}
        sql = "select id,pid from zz6_info_new "
        result_set = self.db_conn.QueryDict(sql)
        for row in result_set:
            id_pid_dict[row['id']]  = row['pid']
            self.pid_id_dict.setdefault(row['pid'], [])
            self.pid_id_dict[row['pid']].append(row['id'])

        tree_dict = {}
        self._stat(tree_dict, 0)
        
        self.p_lst = []
        self._stat_p(tree_dict, '')

        self.SaveList('a.txt', self.p_lst)
        s_lst = sorted(self.p_lst, key=lambda x:x[0])
        self.SaveList('b.txt', s_lst)


    def calculate_path_level(self):
        city_dict = {}
        sql = "select id,pid from zz6_info_new "
        result_set = self.db_conn.QueryDict(sql)
        for row in result_set:
            city_dict[row['id']] = row['pid']

        for k in city_dict.keys():
            city_id = k
            path = str(k)
            k = city_dict[k]
            while k:
                path = str(k) + "|" + path
                k = city_dict[k]
            level = len(path.split('|'))
            sql = "update zz6_info_new set ext_path = %s, ext_level=%s  where id = %s"
            self.db_conn.Execute(sql, [path, level, city_id])

    def _match(self, n1, n2):
        if n1.find(n2) >= 0:
            return True
        if n2.find(n1) >= 0:
            return True
        return False

    def name_math(self, l1, l2):
        if len(l1) == len(l2):
            for i in range(len(l1)):
                b = self._match(l1[i], l2[i])
                if not b:
                    return False
            return True
        return False

    def calculate_city_id(self):
        cityid_name_dict = {}
        sql = "select * from city_info "
        result_set = self.db_conn.QueryDict(sql)
        for row in result_set:
            cityid_name_dict[row['city_id']] = row['city_name']

        path_name_dict = {}
        for row in result_set:
            path_list = row['path'].split(u'|')
            name_list = []
            for path in path_list:
                name_list.append(cityid_name_dict[path])
            path_name_dict[row['city_id']] = name_list

        zz6_info_dict = {}
        sql = "select id,pid,anchor_text,ext_path from zz6_info_new "
        result_set = self.db_conn.QueryDict(sql)
        for row in result_set:
            zz6_info_dict[row['id']] = row['anchor_text']

        zz6_name_dict = {}
        for row in result_set:
            path_list = row['ext_path'].split(u'|')
            name_list = []
            for path in path_list:
                name_list.append(zz6_info_dict[int(path)])
            zz6_name_dict[row['id']] = name_list

        for sid, name_list in zz6_name_dict.iteritems():
            flag = False
            for city_id, city_name_list in path_name_dict.iteritems():
                b = self.name_math(name_list, city_name_list)
                if b:
                    flag = True
                    sql = "update zz6_info_new set ext_city_id = '%s' where id = %s" % (self.ToString(city_id), self.ToString(sid))
                    logging.info(sql)
                    self.db_conn.Execute(sql)
                    break
            if not flag:
                logging.info("invalid sid: %s" % self.ToString(sid))


    def run(self):
        self.calculate_path_level()

if __name__ == '__main__':
    btlog_init('log_format.log', logfile=True, console=False)
    d = Zz6Formater()
#    d.run()
#    d.stat()
    d.calculate_city_id()
