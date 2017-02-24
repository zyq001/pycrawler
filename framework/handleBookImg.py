#!/usr/bin/python
# -*- coding: UTF-8 -*-
from xml.etree import ElementTree

from aliOss import getInfoBucket, upload2Bucket
from config import dumpDict2Yaml
from dbHelper import getDushuConnPool
from shuqiFliter import defaultImg
from shuqiFliter import getSQBookInfoTextById

hideDict = dict()

def ReplaceImg(bookId, imgUrl, conn, csor):
    try:
        csor.execute('update cn_dushu_book set imgUrl = %s where id = %s', (imgUrl,bookId))
        conn.commit()
    except Exception as e:
        print 'update bookImg exception, bookId: ', bookId, 'exception: ', e


def updateBookImgs(st, end):
    conn,csor = getDushuConnPool()
    if end < 1:
        csor.execute('select id,source,imgUrl from cn_dushu_book where id >= %s ', (st,))
    else:
        csor.execute('select id,source,imgUrl from cn_dushu_book where id >= %s and id <= %s', (st,end))
    conn.commit()

    infoBucket = getInfoBucket()

    results = csor.fetchall()
    for book in results:
        mybookId = book[0]
        source = book[1]
        imgUrl = book[2]

        if 'shuqi' not in source:
            print 'not shuqi book, bookId: ',mybookId, ' , source: ',source
            continue
        sqBookId = int(source.replace('shuqi',''))
        text = getSQBookInfoTextById(sqBookId)

        root = ElementTree.fromstring(text.encode('utf-8'))
        isHide = root.attrib['hide']
        if isHide == 'N' and 'original_' not in imgUrl:
            print 'myBookId ',mybookId, ' sqId', sqBookId,' hide N'
            continue
        bookHideObj = dict()
        bookHideObj['id'] = mybookId
        bookHideObj['sqid'] = sqBookId
        bookHideObj['hide'] = isHide
        hideDict[mybookId] = bookHideObj

        ReplaceImg(mybookId, defaultImg, conn, csor)

        try:
            # infoBucket.put_object(mybookId, text)
            upload2Bucket(infoBucket, str(mybookId) + '.xml', text)
        except Exception as e:
            print mybookId,' upload exception ',e

    dumpDict2Yaml(str(end) + '-hide.yaml', hideDict)

if __name__ == '__main__':
    st = 52399
    # st = 209000
    # end = 0
    end = 0
    import sys
    if len(sys.argv) > 1:
        st = sys.argv[1]
        end = sys.argv[2]
    updateBookImgs(st, end)



