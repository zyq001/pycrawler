#!/usr/bin/python
# -*- coding: UTF-8 -*-
import MySQLdb

from Config import EADHOST, EADPASSWD, ERAHOST, ERAPASSWD


def getConn():

    import MySQLdb
    # 打开数据库连接
    db = MySQLdb.connect(host=EADHOST, port=3306, user="ead",
                         passwd=EADPASSWD, db="tmath", use_unicode=True)

    db.set_character_set('utf8')

    cursor = db.cursor()

    # Enforce UTF-8 for the connection.
    cursor.execute('SET NAMES utf8mb4')
    cursor.execute("SET CHARACTER SET utf8mb4")
    cursor.execute("SET character_set_connection=utf8mb4")

    return cursor,db


def getDushuConn():

    import MySQLdb
    # 打开数据库连接
    db = MySQLdb.connect(host=ERAHOST, port=3306, user="tataera",
                         passwd=ERAPASSWD, db="suixinggou_test", use_unicode=True)

    db.set_character_set('utf8')

    cursor = db.cursor()

    # Enforce UTF-8 for the connection.
    cursor.execute('SET NAMES utf8mb4')
    cursor.execute("SET CHARACTER SET utf8mb4")
    cursor.execute("SET character_set_connection=utf8mb4")

    return cursor,db

def getDushuConnPool():
    from DBUtils.PooledDB import PooledDB

    pool2 = PooledDB(creator=MySQLdb, mincached=1, maxcached=1,
                     host=EADHOST, port=3306, user="ead",
                     passwd=EADPASSWD, db="dushu", use_unicode=True, charset='utf8')
    conn2 = pool2.connection()
    csor2 = conn2.cursor()

    # conn.set_character_set('utf8')
    csor2.execute('SET NAMES utf8')
    csor2.execute("SET CHARACTER SET utf8")
    csor2.execute("SET character_set_connection=utf8")

    return conn2,csor2