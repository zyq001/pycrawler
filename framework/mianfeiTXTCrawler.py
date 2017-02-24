##!/usr/bin/python
# -*- coding: UTF-8 -*-
import hashlib
import json
import random

import MySQLdb

from Config import EADHOST, EADPASSWD
from easouCrawl import insertCapWithCapObj
from framework.htmlParser import getSoupByStr
from networkHelper import getContentWithUA
from shuqi import insertBookWithConn

ua = 'Mozilla/5.0 (Linux; U; Android 4.0; en-us; Xoom Build/HRI39) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13'
capListAPIDeviceInfo = '&soft_id=1&ver=110817&platform=an&placeid=1007&imei=862953036746111&cellid=13&lac=-1&sdk=18&wh=720x1280&imsi=460011992901111&msv=3&enc=666501479540451111&sn=1479540459901111&vc=e8f2&mod=M3'


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


def getBucket():
    import oss2

    # endpoint = OSSINTERNALENDPOINT  # 假设你的Bucket处于杭州区域
    endpoint = OSSINTERNALENDPOINT  # 假设你的Bucket处于杭州区域

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

def handleByMTID(mid):
    baseUrl = 'http://api.yingyangcan.com.cn/interface/ajax/book/getbaseinfo.ajax?contentid='
    capListBaseUrl = 'http://api.yingyangcan.com.cn/interface/ajax/book/getcatalog.ajax?contentid=' + str(mid) \
                     +'&pageindex=1&pagesize=100000000'
    capContentBaseUrl = 'http://api.yingyangcan.com.cn/interface/ajax/book/getcharpter.ajax?chapterindex='#2&contentid=171117'
    bookDetailUrl = 'http://m.yingyangcan.com.cn/interface/template/content/book_detail.vhtml?id=';
    url = baseUrl + str(mid);
    baseInfoContent = getContentWithUA(url,ua)
    if not baseInfoContent:
        baseInfoContent = getContentWithUA(url, ua)
    baseObj = json.loads(baseInfoContent)

    baseData = baseObj['data']
    author = baseData['author']
    title = baseData['name']
    coverUrl = baseData['coverUrl']
    contentUrl = baseData['contentUrl']
    count = baseData['count']
    isOver = baseData['isOver']
    BookType = '连载'
    if isOver == 1:
        BookType = '完结'

    bookDetailHtml = getContentWithUA(bookDetailUrl + str(mid),ua)
    bookDetailSoup = getSoupByStr(bookDetailHtml)
    bookDesc = bookDetailSoup.select_one('#J-desc').get_text().replace('\n','').replace('\t\t','\t')

    bookObj = dict()
    bookObj['subtitle'] = bookDesc
    bookObj['source'] = "" + str(mid)
    bookObj['rawUrl'] = url
    bookObj['title'] = title
    bookObj['chapterNum'] = count
    bookObj['imgUrl'] = coverUrl
    bookObj['author'] = author
    bookObj['size'] = count * 1000
    bookObj['category'] = '仙侠'
    bookObj['type'] = '重生'

    bookObj['bookType'] = BookType

    bookObj['typeCode'] = 4
    bookObj['categoryCode'] = 1

    bookObj['viewNum'] = random.randint(500000,1000000)

    m2 = hashlib.md5()
    forDigest = title + u'#' + author
    m2.update(forDigest.encode('utf-8'))
    digest = m2.hexdigest()

    bookObj['digest'] = digest

    bookObj = insertBookWithConn(bookObj, conn2, csor2)

    # myBookId = bookObj['id']
    #
    for cid in range(1047, count + 1):

        capContentUrl = capContentBaseUrl + str(cid) + '&contentid=' + str(mid)
        capContent = getContentWithUA(capContentUrl,ua)
        if not capContent:
            capContent = getContentWithUA(capContentUrl, ua)
        capListJsonObj = json.loads(capContent)
        if not (capListJsonObj['status'] == 1000 and capListJsonObj['message'] == u'成功'):
            capListJsonObj = json.loads(capContent)
            if not (capListJsonObj['status'] == 1000 and capListJsonObj['message'] == u'成功'):
                continue
        capObj = dict()
        orgContent = capListJsonObj['data']['chapter']
        contentSoup = getSoupByStr(orgContent)
        del contentSoup.body['style']
        content = unicode(contentSoup.body).replace(u'<body>', '').replace(u'</body>', '').replace(u'\n\n', u'\n').replace(u'<br><br>', u'<br>').replace(u'<br\><br\>', u'<br\>')
        capObj['content'] = content
        capObj['title'] = unicode(contentSoup.title.get_text())
        capObj['rawUrl'] = capContentUrl
        # capObj['size'] = int(WordsCount)
        capObj['size'] = len(content)
        capObj['bookId'] = bookObj['id']
        capObj['source'] = bookObj['source']
        capObj['idx'] = cid
        capObj['bookUUID'] = bookObj['digest']

        m2 = hashlib.md5()
        forDigest = bookObj['digest'] + capObj['title'] + u'#' + str(cid)
        m2.update(forDigest.encode('utf-8'))
        digest = m2.hexdigest()
        capObj['digest'] = digest

        capId = insertCapWithCapObj(capObj, conn2, csor2)
        if not capId:
            continue
        upload2Bucket(str(capObj['id']) + '.json', json.dumps(capObj))




# def addGroupMembers():
#     import random
#     count = random.randint(10, 30)
#     for userId in random.sample(range(0, 50), count)

if __name__ == '__main__':
    # handleByMTID(290172)
    # handleByMTID(171117)

    handleByMTID(56561)
    # handleByMTID(183433)
    # handleByMTID(249357)
    # handleByMTID(236565)
    # handleByMTID(87433)

    # for myBookId in (221449, 55255,290172,213005,236565,249357,183433,263175,286271, 293046):
    # for myBookId in (213005,236565,249357,183433,263175,286271, 293046):
    # for myBookId in (286271, 293046):
    #     handleByMTID(myBookId)

    # handleWebsiteNoise(581398, 582410)
    # dealBookComment()

    # import sys
    # handleByMTID(int(sys.argv[1]))
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
