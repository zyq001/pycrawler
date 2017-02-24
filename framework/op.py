##!/usr/bin/python
# -*- coding: UTF-8 -*-
import json
# import multiprocessing
# import random
# from xml.etree import ElementTree

# def print_node(node):
#     '''''打印结点基本信息'''
#     print "=============================================="
#     print "node.attrib:%s" % node.attrib
#     if node.attrib.has_key("age") > 0 :
#         print "node.attrib['age']:%s" % node.attrib['age']
#     print "node.tag:%s" % node.tag
#     print "node.text:%s" % node.text
import MySQLdb
# import time
#
# import re

import oss2
# import yaml
# import hashlib
#
# from config import loadShuQC, loadShuQSeqC, loadCrawledBook
# from easouCrawl import insertBook, getExistsCaps, insertCapWithCapObj, getExistsCapsRawUrlId, \
#     updateContentById
# from executor import getAndParse
# from framework.shuqi import getBucket
# from networkHelper import getContent, getContentWithUA

ua = 'Mozilla/5.0 (Linux; U; Android 4.0; en-us; Xoom Build/HRI39) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13'
capListAPIDeviceInfo = '&soft_id=1&ver=110817&platform=an&placeid=1007&imei=862953036746111&cellid=13&lac=-1&sdk=18&wh=720x1280&imsi=460011992901111&msv=3&enc=666501479540451111&sn=1479540459901111&vc=e8f2&mod=M3'

# donedegest = loadCrawledBook()
# gBookDict = dict()


from DBUtils.PooledDB import PooledDB

pool2 = PooledDB(creator=MySQLdb, mincached=1, maxcached=2,
                host=EADHOST, port=3306, user="ead",
                passwd=EADPASSWD, db="dushu", use_unicode=True, charset='utf8')
conn2 = pool2.connection()
csor2 = conn2.cursor()

# conn.set_character_set('utf8')
csor2.execute('SET NAMES utf8')
csor2.execute("SET CHARACTER SET utf8")
csor2.execute("SET character_set_connection=utf8")


# def recrawlShuqi():
#     csor2.execute("SELECT id,digest,rawUrl from cn_dushu_book where rawUrl like '%shuqi%';")
#
#     res = csor2.fetchAll()
#
#     for book in res:
#         bookId = book[0]
#         bookUUID = book[1]
#         bookUrl = book[2]
#
#         indexBefShuqId = bookUrl


def getBucket():
    import oss2

    endpoint = OSSINTERNALENDPOINT  # 假设你的Bucket处于杭州区域
    # endpoint = OSSINTERNALENDPOINT  # 假设你的Bucket处于杭州区域

    auth = oss2.Auth(OSSUSER, OSSPASSWD)
    bucket = oss2.Bucket(auth, endpoint, 'dushu-content')

    return bucket

def findFromTXT():
    f = open('oldChapNums.txt','r')
    f2 = open('OLDmissBookId.txt','w')
    f3 = open('OLDmoreBookId.txt','w')
    f.readline()
    while 1:
        line = f.readline()
        if not line:
            break
        lineArr = line.split(',')
        bookId = lineArr[0]
        count = int(lineArr[1])
        cnum = int(lineArr[2].replace('\n', ''))
        if count == cnum:
            continue
        if count > cnum:
            print u'多了  ',line
            f3.write(line)
        else:
            print u"不够",line
            f2.write(line)

    f2.flush()
    f2.close()
    f3.flush()
    f3.close()
    f.close()

def hanleDeplCps():
    moreFile = open('OLDmoreBookId2.txt', 'r')

    while 1:
        line = moreFile.readline()
        if not line:
            break
        lineArr = line.split(',')
        bookId = lineArr[0]
        count = int(lineArr[1])
        cnum = int(lineArr[2].replace('\n', ''))
        csor2.execute('select count(*) from cn_dushu_acticle where id > 1714000 and bookId = %s', (bookId,))
        conn2.commit()
        res = csor2.fetchone()
        newCount = res[0]
        if float(newCount) / float(cnum) > 0.8:
            csor2.execute('delete from cn_dushu_acticle where id < 1714000 and bookId = %s', (bookId,))
            conn2.commit()
            print 'del ok,  ',line
        else:
            print 'not good, ',line

def handlShuqiMiss():
    miss = open('missBookId.txt', 'r')
    while 1:
        line = miss.readline()
        if not line:
            break
        lineArr = line.split(',')
        bookId = lineArr[0]
        count = int(lineArr[1])
        cnum = int(lineArr[2].replace('\n', ''))


def oppp(f, t):
    bt = getBucket()
    csor2.execute('select id from cn_dushu_acticle ORDER BY id limit %s,%s',(f,t-f))
    conn2.commit()

    res = csor2.fetchall()
    for c in res:
        cid = c[0]
        try:
            result = bt.get_object(str(cid) + '.json').read()
            jsObj = json.loads(result)
            title = jsObj['title']
            size = jsObj['size']
            csor2.execute('update cn_dushu_acticle set title = %s, size = %s where id = %s',(title,size,cid))
            conn2.commit()
            print cid
            # title = jsObj['title']
        except oss2.exceptions.NoSuchKey as e:
            print('{0} not found: http_status={1}, request_id={2}'.format(cid, e.status, e.request_id))


if __name__ == '__main__':
    # oppp(0, 10000)

    import sys
    oppp(int(sys.argv[1]), int(sys.argv[2]))
    # updateCapDigest()

    # handleWebsiteNoise(581398, 582410)
    # import sys
    # uploadCapByCid(int(sys.argv[1]))
    # uploadCapFromTo(699818, 700000)
    # uploadCapFromTo(int(sys.argv[1]), int(sys.argv[2]))

    # shuqCategory2 = loadShuQC()
    # findFromTXT()
    # hanleDeplCps()
    # start(3980892,shuqCategory2)
    # start(115468,shuqCategory2)

    # shuqiAddInitTmp()
    # startFromCId()
    # shuqiAddInit()
    #
    # f = open('shuqiBookId.log', 'r')
    # f.readline()
    # while 1:
    #     id = f.readline()
    #     if not id:
    #         break
    #     id = id.replace('\n', '')
    #     start(id, shuqCategory2)
    #


    # from multiprocessing import Pool
    #
    # manager = multiprocessing.Manager()
    #
    # # 父进程创建Queue，并传给各个子进程：
    # queue = manager.Queue(maxsize=100)
    #
    # p = Pool(5)
    #
    # p.apply_async(onlyInsertCap, args=(queue,))
    # # p.apply_async(onlyInsertCap, args=(queue,))
    # # p.apply_async(onlyInsertCap, args=(queue,))
    # #
    # startFromCId(p,queue)
    # p.close()
    # p.join()




    # ids = '6692553,4871569,5067938,57392,51602'
    # for bookId in ids.split(','):
    #     start(bookId, shuqCategory2)
    # startFromLatestAjax()
