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

    def info(self):
        city_dict = {}
        sql = "select * from zz6_info "
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
            sql = "update zz6_info set path = %s where id = %s"
            self.db_conn.Execute(sql, [path, city_id])

    def run(self):
        self.info()

if __name__ == '__main__':
    btlog_init('log_format.log', logfile=True, console=False)
    d = Zz6Formater()
    d.run()
