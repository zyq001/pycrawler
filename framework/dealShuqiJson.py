#!/usr/bin/python
# -*- coding: UTF-8 -*-

import json

# from framework.shuqi import insertBookWithConn
import os
import random

import time

import logging

from dbHelper import getDushuConnPool
from aliOss import getBucket

db_dushu = 'cn_dushu_book_temp'
db_acticle = 'cn_dushu_acticle_temp'

def insertBookWithConn(bookObj, connDoc,csorDoc):

    userId = random.randint(1,50)

    updateTime = int(time.time())

    import hashlib

    m2 = hashlib.md5()
    forDigest = bookObj['title'] + u'#' + bookObj['author']
    # forDigest = u'总裁我很忙#jxj季'
    m2.update(forDigest.encode('utf-8'))
    digest =  m2.hexdigest()
    bookObj['digest'] = digest

    if not bookObj.has_key('source'):
        bookObj['source'] = 'yisouxiaoshuo'

    sql = 'insert ignore cn_dushu_book ' \
          # '(categoryCode,typeCode,category,type,userId,title,subtitle,imgUrl,author,updateTime' \
          # ",rawUrl,source,digest,status,viewNum, chapterNum, bookType) values" \
          # "('%s','%s','%s','%s',%d,'%s','%s','%s','%s',%d,'%s','%s','%s',%d,'%s','%s','%s')" \
          # % (bookObj['categoryCode'],bookObj['typeCode'], bookObj['category'], bookObj['type'], userId,bookObj['title']
          #    ,bookObj['subtitle'],bookObj['imgUrl'],bookObj['author'],updateTime, bookObj['rawUrl']
          #    ,bookObj['source'],digest, 11,bookObj['viewNum'],bookObj['chapterNum'],bookObj['bookType'])

    try:
        csorDoc.execute('insert ignore ' + db_dushu +
          '(categoryCode,typeCode,category,type,userId,title,subtitle,imgUrl,author,updateTime' \
          ",rawUrl,source,digest,status,viewNum, chapterNum, bookType, size) values" \
          "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" \
          , (bookObj['categoryCode'],bookObj['typeCode'], bookObj['category'], bookObj['type'], userId,bookObj['title']
             ,bookObj['subtitle'],bookObj['imgUrl'],bookObj['author'],updateTime, bookObj['rawUrl']
             ,bookObj['source'],digest, 11,bookObj['viewNum'],bookObj['chapterNum'],bookObj['bookType'],bookObj['size']))
        # csorDoc.execute('update cn_dushu_book set subtitle = %s where digest = %s'
        #   , (bookObj['subtitle'],digest))
        connDoc.commit()
        # print type(bookObj['title'])
    except Exception as e:
        #     # 发生错误时回滚
        print 'update rollback; maybe exists， ', bookObj['rawUrl'],e
        if connDoc:
            try:
                connDoc.rollback()
            except Exception as ee:
                print 'rollback error : ',bookObj['rawUrl']
        # return None

    # sql2 = 'select id from cn_dushu_book where rawUrl = "%s";' % (bookObj['rawUrl'])
    csorDoc.execute("select id from " + db_dushu + " where rawUrl = %s", (bookObj['rawUrl'],))
    connDoc.commit()

    results = csorDoc.fetchall()

    if not results or len(results) < 1:
        return None
    else:
        bookObj['id'] = results[0][0]
    return bookObj


def insertCapWithCapObj2(capObj, conn2, csor2):
    # sql = "insert ignore cn_dushu_acticle (title,rawUrl,source,content,bookId,idx,digest,size,bookUUID) values" \
    #       "('%s','%s','%s','%s',%d,%d,'%s', %d, '%s')" % (
    #           capObj['title'], capObj['rawUrl'], capObj['source'], capObj['content']
    #           , capObj['bookId'], capObj['idx'], capObj['digest'], capObj['size'], capObj['bookUUID'])
    try:
        csor2.execute("insert ignore " + db_acticle + " (bookId,idx,digest,bookUUID,title,size) values" \
          "(%s,%s,%s,%s,%s,%s)" , (capObj['bookId'], capObj['idx'], capObj['digest'], capObj['bookUUID'], capObj['title'], capObj['size']))
        # csor2.execute("update cn_dushu_acticle set title = %s, size= %s where digest = %s" , (capObj['title'], capObj['size'], capObj['digest'] ))
        conn2.commit()
        # print 'scap， ', capObj['source']+":" + str(capObj['idx'])
            # , ', content: ', capObj['content'][0:15]



    except Exception as e:
        #     # 发生错误时回滚
        print 'mysql ex: ', e
        if conn2:
            try:
                conn2.rollback()
            except Exception as ee:
                print 'rollback error : ', capObj['bookId']
        # return None
    try:
        csor2.execute("select id,bookId from " + db_acticle + " where digest = %s;", (capObj['digest'],))
        conn2.commit()

        sqlObj = csor2.fetchone()
        capId = sqlObj[0]
        bookId = sqlObj[1]

        if bookId != capObj['bookId']:
            print 'update bookId',capId
            # 如果已存在，且bookId不对，更新下，防止错误cap占坑
            csor2.execute("update " + db_acticle + " set bookId = %s where id = %s;", (capObj['bookId'], capId))
            conn2.commit()

        capObj['id'] = capId
        return capId
    except Exception as e:
        #     # 发生错误时回滚
        print 'mysql ex2: ', e
        if conn2:
            try:
                conn2.rollback()
            except Exception as ee:
                print 'rollback error : ', capObj['bookId']
        return None



def fromBookAndCapJsons(path):
    logging.basicConfig(filename=path + '_error' + '.log')
    ossBucket = getBucket()

    conn, csor = getDushuConnPool()


    for dir in os.listdir(path):
        cpath = os.path.join(path, dir)
        if os.path.isdir(cpath):
            dirName = cpath
            bookJsonName = dirName + '.json'
            if not os.path.exists(bookJsonName):
                logging.error(dirName + ' has dir but no book info json')
                continue
            myBookId = uploadBookJsonByFileName(bookJsonName, conn,csor)
            if not myBookId:
                logging.error(dirName + ' didnont insert book succ')
            for capFile in os.listdir(dirName):
                capObj = uploadCapFile(dirName + '/'+ capFile, myBookId, conn, csor)
                if  (capObj and capObj.has_key('id')):
                    ossBucket.put_object(str(capObj['id']) + '.json', json.dumps(capObj))

            print 'ok book: ',dirName

    print 'beautiful done!!'

    return 'ok'

def uploadBookJsonByFileName(bookJsonName,conn, csor):

    bookObj = json.loads(open(bookJsonName, 'r').read())
    bookObj = insertBookWithConn(bookObj, conn, csor)
    if not (bookObj and bookObj.has_key('id')):
        return None
    return bookObj['id']


def uploadCapFile(capFileName,bookId,  conn, csor):
    capObj = json.loads(open(capFileName, 'r').read())
    # print capObj['content'][0:10]
    capObj['bookId'] =bookId
    mycapId = insertCapWithCapObj2(capObj, conn, csor)
    if not mycapId:
        return None
    capObj['id'] = mycapId
    return  capObj


if __name__ == '__main__':
    # fromBookAndCapJsons('/Users/zyq/projects/simpleCrawler/framework/70000')
    import  sys
    if len(sys.argv) > 1:
        path = sys.argv[1]
        fromBookAndCapJsons(path)