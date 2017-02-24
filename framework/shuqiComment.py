##!/usr/bin/python
# -*- coding: UTF-8 -*-
import json
import multiprocessing
import random
import xml
from xml.etree import ElementTree

# def print_node(node):
#     '''''打印结点基本信息'''
#     print "=============================================="
#     print "node.attrib:%s" % node.attrib
#     if node.attrib.has_key("age") > 0 :
#         print "node.attrib['age']:%s" % node.attrib['age']
#     print "node.tag:%s" % node.tag
#     print "node.text:%s" % node.text
import MySQLdb
import time

import re
from dateutil import parser

# import parser
import yaml
import hashlib

from Config import OSSINTERNALENDPOINT, OSSUSER, OSSPASSWD, EADHOST, EADPASSWD
from config import loadShuQC, loadShuQSeqC
from easouCrawl import insertBook, getExistsCaps, insertCapWithCapObj, onlyInsertCap, getExistsCapsRawUrlId, \
    updateContentById
from executor import getAndParse
from networkHelper import getContent, getContentWithUA

ua = 'Mozilla/5.0 (Linux; U; Android 4.0; en-us; Xoom Build/HRI39) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13'
capListAPIDeviceInfo = '&soft_id=1&ver=110817&platform=an&placeid=1007&imei=862953036746111&cellid=13&lac=-1&sdk=18&wh=720x1280&imsi=460011992901111&msv=3&enc=666501479540451111&sn=1479540459901111&vc=e8f2&mod=M3'

stopTime = 1483838397002
stopId = 3941890

from DBUtils.PooledDB import PooledDB

pool2 = PooledDB(creator=MySQLdb, mincached=1, maxcached=3,
                host=EADHOST, port=3306, user="ead",
                passwd=EADPASSWD, db="dushu", use_unicode=True, charset='utf8')
conn2 = pool2.connection()
csor2 = conn2.cursor()

# conn.set_character_set('utf8')
csor2.execute('SET NAMES utf8')
csor2.execute("SET CHARACTER SET utf8")
csor2.execute("SET character_set_connection=utf8")


def getBucket():
    import oss2

    endpoint = OSSINTERNALENDPOINT  # 假设你的Bucket处于杭州区域
    # endpoint = OSSINTERNALENDPOINT  # 假设你的Bucket处于杭州区域

    auth = oss2.Auth(OSSUSER, OSSPASSWD)
    bucket = oss2.Bucket(auth, endpoint, 'dushu-content')

    return bucket

bucket = getBucket()

def upload2Bucket(id, obj):

    try:
        bucket.put_object(id, obj)
        print 'succ upload ',id
    except Exception as e:
        print id, ' upload faild ',e


def getBookCommentList(sqbookId):
    baseUrl = 'http://api1.shuqireader.com/reader/bc_bbs_interface.php?bid='
    midUrl = '&bbs=see&bbs_num=20&bbs_rand_num='
    commentList = []
    for i in range(1,4):
        url = baseUrl + str(sqbookId) + midUrl + str(i) + capListAPIDeviceInfo
        commentText = getContentWithUA(url, ua)
        if not (commentText and len(commentText) > 30):
            print 'cap content too short '
            break
        capRoot = ElementTree.fromstring(commentText.encode('utf-8'))
        for comment in capRoot.getiterator('Bbs'):
            commentList.append(comment.attrib)
            # BbsId = comment.attrib['BbsId']
            # BbsIdUserName = comment.attrib['BbsIdUserName']
            # BbsIdUserId = comment.attrib['BbsIdUserId']
            # BbsContent = comment.attrib['BbsContent']
            # BbsTime = comment.attrib['BbsTime']
            #
            # commentObj = json.dumps(comment.attrib)
            #
            # BbsId = comment.attrib['BbsId']
            # BbsId = comment.attrib['BbsId']
            # BbsId = comment.attrib['BbsId']
            # BbsId = comment.attrib['BbsId']
    return commentList

def insertMsgBoj(msgObj):
    try:
        csor2.execute("""insert ignore cn_dushu_quanzimsg (id,targetId,fromId,updateTime,source,content) values (%s,%s,%s,%s,%s,%s)"""
                      , (msgObj['id'],msgObj['targetId'],msgObj['fromId'],msgObj['updateTime'],msgObj['source'],msgObj['content']))
        conn2.commit()
        print 'insert ok, bid: ',msgObj['targetId'], ', id: ', msgObj['id'], ', content: ',msgObj['content']
    except Exception as e:
    #     # 发生错误时回滚
        print 'mysql ex: ', e

def dealBookComment(begin = 118911):
    # d = parser.parse('08-15 22:55')
    # d.fromtimestamp()
    # begin = 75487

    # try:
    #     csor2.execute("""select count(*) from cn_dushu_book where source = 'shuqi' """)
    #     conn2.commit()
    # except Exception as e:
    # #     # 发生错误时回滚
    #     print 'mysql ex: ', e
    # sum = csor2.fetchone()[0]
    while 1:
        try:
            csor2.execute("""select id,rawUrl from cn_dushu_book where  id > %s and id <= %s ORDER by id desc """, (begin, begin + 10000))
            conn2.commit()
        except Exception as e:
        #     # 发生错误时回滚
            print 'mysql ex: ', e
        begin = begin + 10000
        results = csor2.fetchall()
        if len(results) < 1:
            print 'done, bid:'
            return
        for book in results:
            bookId = book[0]
            rawUrl = book[1]
            if rawUrl.find('bookId=') < 0:
                continue
            sqBookId = rawUrl[rawUrl.find('bookId=') + 7:]
            try:
                dealCommsByBookId(bookId, sqBookId)
            except Exception as e:
                print 'deal commens error, skip bid:',bookId, ', error:',e


def dealCommsByBookId(bookId, sqBookId):
    try:
        comments = getBookCommentList(sqBookId)
    except Exception as e:
        print 'get commentList error, skip, bid:',bookId, ', error:',e
        return
    except xml.parsers.expat.ExpatError as error:
        print 'get commentList error, skip, bid:',bookId, ', error:',error
        return
    if len(comments) < 1:
        print 'no comments, bid:',bookId
        return
    # import time
    for comm in comments:
        BbsId = comm['BbsId']
        # BbsIdUserName = comm['BbsIdUserName']
        # BbsIdUserId = comm['BbsIdUserId']
        BbsContent = comm['BbsContent']
        if u'书旗' in BbsContent or u'更新' in BbsContent:
            print 'has noise, bid: ',bookId, ' content: ',BbsContent
            continue
        BbsTime = 0
        try:
            BbsTime = long(time.mktime(parser.parse(comm['BbsTime']).timetuple()) * 1000)
        except Exception as e:
            # print 'parse time error, ', e
            print 'parse time error, skip, bid:',bookId,' id:',BbsId, ', bbstime: ',comm['BbsTime'], ', e:',e
            continue
        if BbsTime > time.time() * 1000:
            BbsTime = BbsTime - 365 * 24 * 3600 * 1000
        if BbsTime > stopTime:
            print 'too new ,bid: ',bookId, ', id: ',BbsId, ': time ',BbsTime, ' content:',BbsContent
            continue

        if long(BbsId) > stopId:
            print 'id too new ,bid: ',bookId, ', id: ',BbsId, ': time ',BbsTime, ' content:',BbsContent
            continue
        msgObj = dict()
        msgObj['id'] = BbsId
        msgObj['fromId'] = random.randint(1, 50)
        msgObj['targetId'] = bookId
        msgObj['updateTime'] = BbsTime
        msgObj['source'] = 'shuqi'
        msgObj['content'] = BbsContent
        insertMsgBoj(msgObj)


# def addGroupMembers():
#     import random
#     count = random.randint(10, 30)
#     for userId in random.sample(range(0, 50), count)

if __name__ == '__main__':
    # handleWebsiteNoise(581398, 582410)
    import sys
    if len(sys.argv) > 1:
        dealBookComment(int(sys.argv[1]))
    elif len(sys.argv) > 3:
        dealBookComment(int(sys.argv[1]))
        stopTime = long(sys.argv[2])
        stopId = long(sys.argv[3])
    else:
        dealBookComment()
    # import sys
    # dealCommsByBookId(74388, 5820364)
    # uploadCapFromTo(313482, 1200000)
    # uploadCapFromTo(int(sys.argv[1]), int(sys.argv[2]))

    # shuqCategory2 = loadShuQC()

    # start(3980892,shuqCategory2)
    # start(115468,shuqCategory2)
    # from multiprocessing import Pool
    #
    # manager = multiprocessing.Manager()
    #
    # # 父进程创建Queue，并传给各个子进程：
    # queue = manager.Queue(maxsize=100)
    #
    # p = Pool(3)
    #
    # p.apply_async(onlyInsertCap, args=(queue,))
    # p.apply_async(onlyInsertCap, args=(queue,))
    # # p.apply_async(onlyInsertCap, args=(queue,))
    #
    # startFromCId(p,queue)
    # ids = '6692553,4871569,5067938,57392,51602'
    # for bookId in ids.split(','):
    #     start(bookId, shuqCategory2)
    # startFromLatestAjax()
