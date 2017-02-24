#!/usr/bin/python
# -*- coding: UTF-8 -*-
import MySQLdb
from DBUtils.PooledDB import PooledDB

from Config import EADHOST, EADPASSWD


def nightMove(tableName):
    pool2 = PooledDB(creator=MySQLdb, mincached=1, maxcached=1,
                    host=EADHOST, port=3306, user="ead",
                    passwd=EADPASSWD, db="dushu", use_unicode=True, charset='utf8')
    conn2 = pool2.connection()
    csor2 = conn2.cursor()

    # conn.set_character_set('utf8')
    csor2.execute('SET NAMES utf8')
    csor2.execute("SET CHARACTER SET utf8")
    csor2.execute("SET character_set_connection=utf8")

    # 转移十万条
    csor2.execute('insert ignore cn_dushu_acticle select * from cn_dushu_acticle')