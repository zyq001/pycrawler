##!/usr/bin/python
# -*- coding: UTF-8 -*-
import hashlib
import random
from xml.etree import ElementTree
import traceback
import logging

from config import loadShuQC, capListAPIDeviceInfo, loadBloomFromFile, loadBloomBooks, dumpBloomToFile, bloomDumpBooks, \
    bloomDumpCapsName
from dbHelper import getDushuConnPool
from dealShuqiJson import insertBookWithConn, insertCapWithCapObj2
from aliOss import getBucket, upload2Bucket
from networkHelper import getContentWithUA, getContent
# from shuqi2 import loadExistsSQId

defaultImg = 'http://tata-img.oss-cn-shanghai.aliyuncs.com/book-default.jpg'
sqDefaultImg = 'http://oss-asq-img.11222.cn/bcv/middle/201603311324445820.jpg'

ua = 'Mozilla/5.0 (Linux; U; Android 4.0; en-us; Xoom Build/HRI39) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13'
db_dushu = 'cn_dushu_book_temp'
db_acticle = 'cn_dushu_acticle_temp'

# donedegest = dict()
def isSQBidNull():
    print 'ok'


def sizeStr2Int(size):
    size = size.strip()
    carryName = size[-1:]
    carry = 1000
    if carryName == u'万':
        carry = 10000
    elif carryName == u'千':
        carry = 1000
    count = 1
    try:
        # count = float(carryName[0: -1])
        countStr = size[0: -1]
        import ast
        count = ast.literal_eval(countStr)
    except Exception as e:
        print '计算书的大小出错',e
    return int(carry * count)

def getBookObjFromSQid(id, shuqCategory):
    root = getSQBookInfoXMLById(id)
    BookType = ''
    if len(root.getiterator('BookType')) > 0:
        BookType = root.getiterator('BookType')[0].text
    category = ''
    if len(root.getiterator('NickName')) > 0:
        category = root.getiterator('NickName')[0].text
    tag = ''
    if len(root.getiterator('ShortNickName')) > 0:
        tag = root.getiterator('ShortNickName')[0].text
    tagId = 0
    if root.getiterator('NickId') and len(root.getiterator('NickId')) > 0 and root.getiterator('NickId')[0].text:
        tagId = int(root.getiterator('NickId')[0].text)
    firstCid = 0
    if root.getiterator('ChapteridFirst') and len(root.getiterator('ChapteridFirst')) > 0 and root.getiterator('ChapteridFirst')[0].text:
        firstCid = int(root.getiterator('ChapteridFirst')[0].text)

    if (not BookType) and (not category) and (not tag) and (not tagId):
        return None,None
    categoryId = 0
    if shuqCategory.has_key(tag):
        if shuqCategory[tag]['id'] and len(shuqCategory[tag]['id']) > 0:
            categoryId = int(shuqCategory[tag]['id'])
    size = 1
    if root.getiterator('Size') and len(root.getiterator('Size')) > 0:
        strSize = root.getiterator('Size')[0].text
        size = sizeStr2Int(strSize)
    NumChapter = 1
    if root.getiterator('NumChapter') and len(root.getiterator('NumChapter')) > 0 and root.getiterator('NumChapter')[0].text:
        NumChapter = int(root.getiterator('NumChapter')[0].text)
    source = 'shuqi' + str(id)
    subtitle = root.getiterator('Description')[0].text
    title = root.getiterator('BookName')[0].text
    author = root.getiterator('Author')[0].text
    imgurl = root.getiterator('ImageExists')[0].text
    if imgurl:
        imgurl = imgurl.replace(sqDefaultImg, defaultImg)
    certainBookUrl = 'http://api.shuqireader.com/reader/bc_cover.php?bookId=' + str(id)
    if not title or len(title) < 1 or len(author) < 1:
        return None,None
    bookObj = dict()
    bookObj['subtitle'] = subtitle
    bookObj['source'] = source
    bookObj['rawUrl'] = certainBookUrl
    bookObj['title'] = title
    bookObj['chapterNum'] = NumChapter
    bookObj['imgUrl'] = imgurl
    bookObj['author'] = author
    bookObj['size'] = size
    bookObj['category'] = tag
    bookObj['type'] = category
    bookObj['bookType'] = BookType
    # bookObj['typeCode'] = 100 + tagId
    # bookObj['categoryCode'] = 100 + categoryId
    bookObj['typeCode'] = tagId
    bookObj['categoryCode'] = categoryId
    bookObj['firstCid'] = firstCid
    bookObj['viewNum'] = 0

    m2 = hashlib.md5()
    forDigest = title + u'#' + author
    m2.update(forDigest.encode('utf-8'))
    digest = m2.hexdigest()
    bookObj['digest'] = digest
    return bookObj, digest


def getSQBookInfoXMLById(id):
    text = getSQBookInfoTextById(id)
    if not (text and len(text) > 160):
        return None
    root = ElementTree.fromstring(text.encode('utf-8'))
    return root


def getSQBookInfoTextById(id):
    bookInfoAPI = 'http://api.shuqireader.com/reader/bc_cover.php?bookId=' + str(
        id) + '&book=same&book_num=5&bbs=pinglun&bbs_num=8&bbs_rand_num=1&lastchaps=1&ItemCount=3&soft_id=1&ver=110817&platform=an&placeid=1007&imei=862953036746111&cellid=13&lac=-1&sdk=18&wh=720x1280&imsi=460011992901111&msv=3&enc=666501479540451111&sn=1479540459901111&vc=e8f2&mod=M3'
    text = getContent(bookInfoAPI)
    return text


def getCapContentObj(bookId, capId):
    pageIndex = 1
    capListAPIBase = 'http://api.shuqireader.com/reader/bc_showchapter.php?bookId='

    capApi = capListAPIBase + str(bookId) + '&chapterid=' + str(capId) + '&pageIndex=' + str(pageIndex) + '&bg=0'
    capText = getContentWithUA(capApi, ua)

    if not (capText and len(capText) > 30):
        print 'cap content too short ,return none'
        # delBookById(mysqlBKid)
        return None
    capRoot = ElementTree.fromstring(capText.encode('utf-8'))

    ChapterName = ''
    if len(capRoot.getiterator('ChapterName')) > 0:
        ChapterName = capRoot.getiterator('ChapterName')[0].text

    ChapterContent = ''
    if len(capRoot.getiterator('ChapterContent')) > 0:
        ChapterContent = capRoot.getiterator('ChapterContent')[0].text
    if not ChapterContent:
        capText = getContentWithUA(capApi, ua)

        if not (capText and len(capText) > 30):
            print 'cap content too short , ,return none'
            # delBookById(mysqlBKid)
            return None
        capRoot = ElementTree.fromstring(capText.encode('utf-8'))

        ChapterName = ''
        if len(capRoot.getiterator('ChapterName')) > 0:
            ChapterName = capRoot.getiterator('ChapterName')[0].text

        ChapterContent = ''
        if len(capRoot.getiterator('ChapterContent')) > 0:
            ChapterContent = capRoot.getiterator('ChapterContent')[0].text
    if not ChapterContent:
        return None
    ChapterContent = ChapterContent.strip()

    if(ChapterContent.startswith('http') and len(ChapterContent) < 250):
        print 'cap content is url , ,return none', bookId, ' : ',ChapterContent
        return None

    WordsCount = ''
    if len(capRoot.getiterator('WordsCount')) > 0:
        WordsCount = capRoot.getiterator('WordsCount')[0].text

    PageCount = 1
    if len(capRoot.getiterator('PageCount')) > 0:
        PageCount = int(capRoot.getiterator('PageCount')[0].text)
    if PageCount > 1:
        for i in range(2, PageCount + 1):
            pageIndex = i
            capApi2 = capListAPIBase + str(bookId) + '&chapterid=' + str(capId) + '&pageIndex=' + str(
                pageIndex) + '&bg=0' + capListAPIDeviceInfo
            capText2 = getContentWithUA(capApi2, ua)

            if not (capText2 and len(capText2) > 160):
                return
            capRoot2 = ElementTree.fromstring(capText2.encode('utf-8'))
            ChapterContent2 = ''
            if len(capRoot2.getiterator('ChapterContent')) > 0:
                ChapterContent2 = capRoot2.getiterator('ChapterContent')[0].text
            ChapterContent = ChapterContent + ChapterContent2

    capObj = dict()
    capObj['content'] = ChapterContent.replace(u'***求收藏***','').replace(u'***（求收藏）***','').replace(u'求收藏','')
    capObj['title'] = ChapterName
    capObj['rawUrl'] = capApi[0:200]
    # capObj['size'] = int(WordsCount)
    capObj['size'] = len(capObj['content'])
    return capObj
    # return 'ok'


def getShuqiCapList(bookId):

    capList = []

    pageIndex = 1
    capListAPIBase = 'http://api.shuqireader.com/reader/bc_chapter.php?pagesize=40&bookId='


    capListAPI = capListAPIBase + str(bookId) + '&pageIndex=' + str(pageIndex) + capListAPIDeviceInfo

    text = getContentWithUA(capListAPI, ua)

    if not (text and len(text) > 160):
        return

    root = ElementTree.fromstring(text.encode('utf-8'))

    gatherId = root.getiterator('BookInfos')[0].attrib['GatherId']
    TotalCount = int(root.getiterator('BookInfos')[0].attrib['TotalCount'])
    # if TotalCount > 40:
    topPageCount = TotalCount / 40 + 2#分页的总数
    for i in range(1, topPageCount):#如果没有分页，i只会等于1
        if i == 1:
            pageRoot = root
        else:
            pageApi = capListAPIBase + str(bookId) + '&pageIndex=' + str(i) + capListAPIDeviceInfo
            pageText = getContentWithUA(pageApi, ua)

            if not (pageText and len(pageText) > 160):
                return
            pageRoot = ElementTree.fromstring(pageText.encode('utf-8'))

        if gatherId and gatherId != '':#有子目录
            for book in pageRoot.getiterator('Book'):
                vId = book.attrib['ChapterId']

                secondApi = capListAPI + '&vid=' + str(vId)#子目录的url
                textSon = getContentWithUA(secondApi, ua)#子目录的内容

                xmlSon = ElementTree.fromstring(textSon.encode('utf-8')) #子目录的xml

                sonTotalCount = int(xmlSon.getiterator('BookInfos')[0].attrib['TotalCount']) #子目录的记录总数
                sonPageCount = sonTotalCount / 40 + 2#子目录的分页总数
                for j in range(1, sonPageCount):#遍历子目录的每一页，如果没有分页，会只遍历第一页
                    if j == 1:
                        sonpageRoot = xmlSon#第一页不需要再请求url
                    else:
                        morePageApi = capListAPIBase + str(bookId) + '&pageIndex=' + str(j) + capListAPIDeviceInfo \
                                  + '&vid=' + str(vId)#子目录的分页url
                        morePageText = getContentWithUA(morePageApi, ua)
                        sonpageRoot = ElementTree.fromstring(morePageText.encode('utf-8'))  # 子目录的xml

                    for realCap in sonpageRoot.getiterator('Book'):
                        realCapId = realCap.attrib['ChapterId']
                        # dealCap(bookId, realCapId)
                        capList.append(realCapId)

        else:#没有二级目录，不需要请求信的api，所有不需考虑分页
            for realCap in pageRoot.getiterator('Book'):
                realCapId = realCap.attrib['ChapterId']
                # dealCap(bookId, realCapId)
                capList.append(realCapId)

    return capList

def updateBookSize(bookObj, connDoc,csorDoc):
    try:
        csorDoc.execute("update " + db_dushu + "  set size = %s where id = %s " , (bookObj['size'], bookObj['id']))
        # csor2.execute("update cn_dushu_acticle set title = %s, size= %s where digest = %s" , (capObj['title'], capObj['size'], capObj['digest'] ))
        connDoc.commit()
        # print 'scap， ', capObj['source']+":" + str(capObj['idx'])
            # , ', content: ', capObj['content'][0:15]



    except Exception as e:
        #     # 发生错误时回滚
        print 'update book size mysql ex: ', e
        if connDoc:
            try:
                connDoc.rollback()
            except Exception as ee:
                print 'rollback error : ', bookObj['id']

def bookAndCaps(bookDigest, capDegest):
    import sys, os, json


    st = 4000000
    end = 4010000
    if len(sys.argv) > 2:
        st = int(sys.argv[1])
        end = int(sys.argv[2])

    # if getCapContentObj(4342799,300190):
    #     print 'ok'

    shuqCategory2 = loadShuQC()


    # st = 60000
    # end = 70000
    logging.basicConfig(filename=str(end) + '_error' + '.log')

    global db_dushu,db_acticle
    if len(sys.argv) > 3:
        db_dushu = sys.argv[3]
        db_acticle = sys.argv[4]

    conn, csor = getDushuConnPool()
    ossBucket = getBucket()


    # nullBookIds = open('nullSQID.txt', 'r')
    # nullIdSet = set()
    # while 1:
    #     sqid = nullBookIds.readline()
    #     if not sqid:
    #         break
    #     nullIdSet.add(int(sqid.replace('\n', '')))

    # bloom = loadExistsSQId()
    # seq = range(st, end)
    print 'start from shuqi id ', st, ' to ', end, '; insert into ', db_dushu, ', and ', db_acticle
    idx = st
    carry = 10000
    # if idx =

    while idx < end:

        if idx + carry  >= end:
            seq = range(idx, end + 1)
        else:
            seq = range(idx, idx + carry + 1)

        random.shuffle(seq)
        #
        for sqBid in seq:
            # print sqBid
            # if sqBid in nullIdSet:
            #     continue
            if  'shuqi' + str(sqBid) in bookDigest:
                print 'book already crawled, ',sqBid
                continue
            try:
                # start(sqBid, shuqCategory2)
                bookObj, digest = getBookObjFromSQid(sqBid, shuqCategory2)
                if not bookObj:
                    print 'bookNull : ', sqBid
                    continue
                firstCid = bookObj['firstCid']
                # bookFileName = nowDir + '/' + dirName + '/' + str(sqBid) + '.json'
                # bookCapsDir = nowDir + '/' + dirName + '/' + str(sqBid)

                if not getCapContentObj(sqBid, firstCid):
                    print 'bookContentNull : ', sqBid

                    continue

                # json.dump(bookObj, open(bookFileName, 'w'))
                bookObj = insertBookWithConn(bookObj, conn, csor)

                if not (bookObj and bookObj.has_key('id')):
                    logging.error(str(sqBid) + ' insert into db fail, skipped')
                    continue

                capList = getShuqiCapList(sqBid)
                if not capList or len(capList) < 1:
                    print 'caplist null, ',str(capId),' del bookObj'
                    # os.remove(bookFileName)
                    continue

                # if not os.path.exists(bookCapsDir):
                #     os.mkdir(bookCapsDir)

                capOkCount = 0
                bookSize = 0
                for j in range(0, len(capList)):
                    capId = capList[j]
                    try:
                        capObj = getCapContentObj(sqBid, capId)
                    except Exception as e:
                        print 'get cap content exception, bid: ',str(sqBid),', cid: ',str(capId), ':  ', e
                        try:
                            capObj = getCapContentObj(sqBid, capId)
                        except Exception as ee:
                            print 'get cap content exception, bid: ', str(sqBid), ', cid: ', str(capId), ':  ', ee

                            s = traceback.format_exc()
                            logging.error(s)
                    except IOError as error:
                        print 'get cap content ioerror, bid: ',str(sqBid),', cid: ',str(capId), ':  ', error

                        try:
                            capObj = getCapContentObj(sqBid, capId)
                        except Exception as e:
                            print 'get cap content exception, bid: ', str(sqBid), ', cid: ', str(capId), ':  ', e

                            s = traceback.format_exc()
                            logging.error(s)


                    if not capObj:
                        print 'capObj null ,bid: ',str(sqBid), ' cid: ',str(capId)
                        continue
                    capObj['bookId'] = bookObj['id']
                    capObj['source'] = bookObj['source']
                    capObj['idx'] = j
                    capObj['bookUUID'] = bookObj['digest']

                    m2 = hashlib.md5()
                    forDigest = bookObj['digest'] + capObj['title'] + u'#' + str(j)
                    m2.update(forDigest.encode('utf-8'))
                    digest = m2.hexdigest()
                    capObj['digest'] = digest

                    if digest in capDegest:
                        print 'cap exists, bid: ',str(sqBid), ' ,cid: ',str(capId)
                        continue

                    # capFile = open(bookCapsDir + '/' + str(capId) + '.json', 'w')
                    # json.dump(capObj, capFile)
                    # capFile.close()
                    mycapId = insertCapWithCapObj2(capObj, conn, csor)
                    if not mycapId:
                        return None
                    capObj['id'] = mycapId
                    # if (capObj and capObj.has_key('id')):
                    # ossBucket.put_object(str(capObj['id']) + '.json', json.dumps(capObj))
                    upload2Bucket(ossBucket,str(capObj['id']) + '.json', json.dumps(capObj))

                    bookSize = bookSize + capObj['size']

                    capOkCount = capOkCount + 1
                    capDegest.add(digest)

                # if capOkCount < 1:
                #     print 'finally, no caps dumped, del book',sqBid
                #     logging.error('bookOk, but finally no caps dumped, del book',sqBid)
                    # try:
                    #     os.rmdir(bookCapsDir)
                    #     os.remove(bookFileName)
                    #
                    # except OSError as error:
                    #
                    #     s = traceback.format_exc()
                    #     logging.error(sqBid, ' cid:',capId, 'thought the cap dir is null, but actuallt not ,so keep it')
                    #     logging.error(s)

                # bookObj['size'] = bookSize
                # updateBookSize(bookObj, conn, csor)
                print 'bookOk : ', sqBid
                bookDigest.add('shuqi' + str(sqBid))



            except Exception as e:
                print sqBid, ':  ', e
                s = traceback.format_exc()
                logging.error(s)
            except IOError as e2:
                print sqBid, ':  ', e2
                s = traceback.format_exc()
                logging.error(s)
                # bloom.add('shuqi' + str(sqBid))

        idx = idx + carry
        # if idx > end:
        #     idx = end
        # dumpBloomToFile(bookDigest, bloomDumpBooks)
        # dumpBloomToFile(capDegest, bloomDumpCapsName)
    print 'Done!!!!'

if __name__ == "__main__":
    capDigests = loadBloomFromFile()
    bookDigests = loadBloomBooks()
    bookAndCaps(bookDigests, capDigests)