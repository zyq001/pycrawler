##!/usr/bin/python
# -*- coding: UTF-8 -*-
import hashlib
import sys

# from framework.shuqi import ua
ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36'
reload(sys)
sys.setdefaultencoding( "utf-8" )

import random
import urlparse

import MySQLdb

from config import loadCategory
from executor import getAndParse
from htmlParser import getSoupByStr
from networkHelper import getContent, getContentWithUA
import time
queue = None
p = None
# import Queue
# queue = Queue.Queue()

DefaultImg = 'http://oss.tatatimes.com/color/default-cover.jpg'


# import pgdb # import used DB-API 2 module
from DBUtils.PooledDB import PooledDB

pool = PooledDB(creator=MySQLdb, mincached=1 , maxcached=2 ,
                host=EADHOST, port=3306, user="ead",
                passwd=EADPASSWD, db="dushu", use_unicode=True, charset='utf8')
conn = pool.connection()
csor = conn.cursor()

# conn.set_character_set('utf8')
csor.execute('SET NAMES utf8')
csor.execute("SET CHARACTER SET utf8")
csor.execute("SET character_set_connection=utf8")

def insertBook(bookObj):

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
        csor.execute('insert ignore cn_dushu_book ' \
          '(categoryCode,typeCode,category,type,userId,title,subtitle,imgUrl,author,updateTime' \
          ",rawUrl,source,digest,status,viewNum, chapterNum, bookType) values" \
          "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" \
          , (bookObj['categoryCode'],bookObj['typeCode'], bookObj['category'], bookObj['type'], userId,bookObj['title']
             ,bookObj['subtitle'],bookObj['imgUrl'],bookObj['author'],updateTime, bookObj['rawUrl']
             ,bookObj['source'],digest, 11,bookObj['viewNum'],bookObj['chapterNum'],bookObj['bookType']))
        conn.commit()
        print 'succ book, ',bookObj['title']
    except Exception as e:
        #     # 发生错误时回滚
        print 'update rollback; maybe exists， ', bookObj['rawUrl'],e
        if conn:
            try:
                conn.rollback()
            except Exception as ee:
                print 'rollback error : ',bookObj['rawUrl']
        # return None

    # sql2 = 'select id from cn_dushu_book where rawUrl = "%s";' % (bookObj['rawUrl'])
    csor.execute("select id from cn_dushu_book where rawUrl = %s", (bookObj['rawUrl'],))
    conn.commit()

    results = csor.fetchall()

    if not results or len(results) < 1:
        return None
    else:
        bookObj['id'] = results[0][0]
    return bookObj

    #
    #
    # checkCapsSql = 'select idx, length(content) from cn_dushu_acticle where bookId = %d' % (bookObj['id'])
    # try:
    #     csor.execute(checkCapsSql)
    #     conn.commit()
    #     results = csor.fetchall()
    #
    #     if not results or len(results) < 1:
    #         print 'cap leaks,, bookName：', bookObj['title']
    #         return bookObj
    #     else:
    #         capCount = len(results)
    #         if capCount < bookObj['chapterNum']:
    #             print 'cap leaks,, bookName：',bookObj['title']
    #             return results #有返回就表明需要抓取章节
    #         else:
    #             print 'caps ok, skipped'
    #             return None
    # except Exception as e:
    #     #     # 发生错误时回滚
    #     print 'check cap count failed ,skip', e


def getExistsCaps(bookId):
    checkCapsSql = 'select idx, length(content) from cn_dushu_acticle where bookId = %d' % (bookId)
    try:
        csor.execute(checkCapsSql)
        conn.commit()
        results = csor.fetchall()

        if not results or len(results) < 1:
            print 'no caps,, bookId：', bookId
            return None
        else:
            return results
    except Exception as e:
        #     # 发生错误时回滚
        print 'check cap count failed ,skip', e
def getExistsCapsRawUrlId(bookId):
    checkCapsSql = 'select id,rawUrl from cn_dushu_acticle where bookId = %d' % (bookId)
    try:
        csor.execute(checkCapsSql)
        conn.commit()
        results = csor.fetchall()

        if not results or len(results) < 1:
            print 'no caps,, bookId：', bookId
            return None
        else:
            return results
    except Exception as e:
        #     # 发生错误时回滚
        print 'check cap count failed ,skip', e

def insertCap(bookObj, capUrl, capName, idx, queue):

    title = capName
    rawUrl = capUrl
    content,source = getAndParse(capUrl)
    if not (content and source):
        print 'no content got, fill with temp ,capUrl : ', capUrl

        content = '暂缺，请稍后再来'
        source = ''

    bookId = bookObj['id']
    size = len(content)
    bookUUID = bookObj['digest']

    import hashlib

    m2 = hashlib.md5()
    forDigest = capName + u'#' + source
    m2.update(forDigest.encode('utf-8'))
    digest =  m2.hexdigest()

    capObj = dict()
    capObj['title'] = title
    capObj['rawUrl'] = rawUrl
    capObj['source'] = source
    capObj['content'] = content
    capObj['bookId'] = bookId
    capObj['idx'] = idx
    capObj['digest'] = digest
    capObj['size'] = size
    capObj['bookUUID'] = bookUUID

    queue.put(capObj)

    # onlyInsertCap(capObj)


def onlyInsertCap(queue):
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


    while True:
        capObj = queue.get()
        # print i, capObj['source'] + capObj['idx']
        insertCapWithCapObj(capObj, conn2, csor2)


def insertCapWithCapObj(capObj, conn2, csor2):
    # sql = "insert ignore cn_dushu_acticle (title,rawUrl,source,content,bookId,idx,digest,size,bookUUID) values" \
    #       "('%s','%s','%s','%s',%d,%d,'%s', %d, '%s')" % (
    #           capObj['title'], capObj['rawUrl'], capObj['source'], capObj['content']
    #           , capObj['bookId'], capObj['idx'], capObj['digest'], capObj['size'], capObj['bookUUID'])
    try:
        csor2.execute("insert cn_dushu_acticle_temp (bookId,idx,digest,bookUUID,title,size) values" \
          "(%s,%s,%s,%s,%s,%s)" , (capObj['bookId'], capObj['idx'], capObj['digest'], capObj['bookUUID'], capObj['title'], capObj['size']))
        # csor2.execute("update cn_dushu_acticle set title = %s, size= %s where digest = %s" , (capObj['title'], capObj['size'], capObj['digest'] ))
        conn2.commit()
        print 'scap， ', capObj['source']+":" + str(capObj['idx']), ', content: ', capObj['content'][0:15]



    except Exception as e:
        #     # 发生错误时回滚
        print 'mysql ex: ', e
        if conn2:
            try:
                conn2.rollback()
            except Exception as ee:
                print 'rollback error : ', capObj['bookId']
        return None
    try:
        csor2.execute("select id,bookId from cn_dushu_acticle_temp where digest = %s;", (capObj['digest'],))
        conn2.commit()

        sqlObj = csor2.fetchone()
        capId = sqlObj[0]
        bookId = sqlObj[1]

        if bookId != capObj['bookId']:
            print 'update bookId',capId
            # 如果已存在，且bookId不对，更新下，防止错误cap占坑
            csor2.execute("update cn_dushu_acticle_temp set bookId = %s where id = %s;", (capObj['bookId'], capId))
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


def easouCrawl():
    catDict = loadCategory()
    shukuUrl = 'http://b.easou.com/w/booklib/all_c_l_2_0_1.html' #已完结
    shukuHtml = getContent(shukuUrl)

    if not shukuHtml:
        return

    shukuSoup = getSoupByStr(shukuHtml)

    conds = shukuSoup.select('.condition')

    if not conds or len(conds) < 1:
        print 'shuku error, stop '
        return
    for cat in conds[1].select('.n a'):#遍历大类的'全部'页

        catUrl = urlparse.urljoin(shukuUrl, cat['href'])

        topCatName = cat.get_text()

        booklistHtml = getContent(catUrl) #大类的'全部'页
        if not booklistHtml:
            continue

        booklistSoup = getSoupByStr(booklistHtml)
        Catconds = booklistSoup.select('.condition')
        if not Catconds or len(Catconds) < 1:
            print 'category error, skip ，', cat.get_text()
            continue
        for tag in Catconds[2].select('.n a'):  # 先遍历大类下面的标签页

            tagUrl = urlparse.urljoin(catUrl, tag['href'])
            tagName = tag.get_text()

            if catDict.has_key(tagName):
                tagObj = catDict[tagName]
            else:
                tagObj = catDict[u'全部']

            dealTagPage(shukuUrl, tagName, tagObj, tagUrl)
            for n in range(2, 5):
                dealTagPage(shukuUrl, tagName, tagObj, tagUrl.replace('1.html', str(n) + '.html'))

        tag2 = Catconds[2].select('.all a')
        tagUrl = urlparse.urljoin(catUrl, tag2['href'])
        tagName = u'其他' + tag2.get_text()
        if catDict.has_key(tagName):
            tagObj = catDict[tagName]
        else:
            tagObj = catDict[u'全部']

        dealTagPage(shukuUrl, tagName, tagObj, tagUrl)
        for n in range(2, 5):
            dealTagPage(shukuUrl, tagName, tagObj, tagUrl.replace('1.html', str(n) + '.html'))




def dealTagPage(shukuUrl, tagName, tagObj, tagUrl):
    tagBooksHtml = getContent(tagUrl)
    if not tagBooksHtml:
        return
    tagBooksSoup = getSoupByStr(tagBooksHtml)
    for book in tagBooksSoup.select('.listcontent .name a'):
        t = random.randint(60, 100)
        time.sleep(t)
        bookUrl = urlparse.urljoin(tagUrl, book['href'])  # 是图书搜索结果页
        bookName = book.get_text()

        bookObj = dict()  # 建图书对象，共函数间传递
        bookObj['title'] = bookName
        bookObj['type'] = tagName
        bookObj['typeCode'] = tagObj['typeCode']
        bookObj['categoryCode'] = tagObj['categoryCode']
        bookObj['category'] = tagObj['category']

        # bookId, bookUUID = insertBook(bookObj) #Book信息入库，获得bookid和uuid

        bookMidHtml = getContent(bookUrl)
        if not bookMidHtml:
            continue
        bookMidSoup = getSoupByStr(bookMidHtml)

        lis = bookMidSoup.select('.resultContent li')

        if not lis or len(lis) < 1:
            print 'get book result list error, skip, book: ', bookUrl, ' tagUrl: ', tagUrl
            continue

        bookLi = lis[0]

        bookImgs = bookLi.select('.imgShow')[0].select('img')
        if bookImgs and len(bookImgs) > 0:
            imgsrc = bookImgs[0]['src']
            bookImg = urlparse.urljoin(bookUrl, imgsrc).replace('http://b.easou.com/w/resources/imgs/pic.gif',
                                                                DefaultImg)

        else:
            bookImg = DefaultImg

        certainBookUrl = urlparse.urljoin(bookUrl, bookLi.select('.name a')[0]['href'])
        author = bookLi.select('.author a')[0].get_text()
        count = bookLi.select('.count')[0].get_text().replace(u'追书人数：', '').replace(u'人追', '')
        bookObj['author'] = author
        bookObj['viewNum'] = count
        bookObj['imgUrl'] = bookImg

        # 具体图书介绍页
        certainBookHtml = getContent(certainBookUrl)
        if not certainBookHtml:
            continue
        certainBookSoup = getSoupByStr(certainBookHtml)

        subtitle = certainBookSoup.select('.desc')[0].get_text()
        source = certainBookSoup.select('.source .t')[0].get_text()

        bookObj['subtitle'] = subtitle
        bookObj['source'] = source
        bookObj['rawUrl'] = certainBookUrl

        agendaLinks = certainBookSoup.select('.dao .category a')

        if not agendaLinks or len(agendaLinks) < 1:
            print 'get book agenda list error, skip, book: ', bookUrl, ' tagUrl: ', tagUrl
            continue

        # 图书目录页
        agendaHtml = getContent(urlparse.urljoin(certainBookUrl, agendaLinks[0]['href']))
        if not agendaHtml:
            continue
        agendaSoup = getSoupByStr(agendaHtml)
        # 终于达到了目录页

        caplist = list()

        caps = agendaSoup.select('.category li a')

        for i in range(0, len(caps)):
            cap = caps[i]
            capUrl = urlparse.urljoin(shukuUrl, cap['href'])
            capName = cap.get_text()
            capObj = {}
            capObj['url'] = capUrl
            capObj['name'] = capName
            caplist.append(capObj)

        pages = agendaSoup.select('.pager a')

        if pages and len(pages) > 0:
            for j in range(0, len(pages) - 1):
                pageA = pages[j]
                nextPageUrl = urlparse.urljoin(bookUrl, pageA['href'])
                # 图书目录页
                agendaHtml2 = getContent(nextPageUrl)

                if not agendaHtml2:
                    continue
                agendaSoup2 = getSoupByStr(agendaHtml2)
                # 终于达到了目录页

                caps = agendaSoup2.select('.category li a')

                for i in range(0, len(caps)):
                    cap = caps[i]
                    capUrl = urlparse.urljoin(shukuUrl, cap['href'])
                    capName = cap.get_text()

                    capObj = {}
                    capObj['url'] = capUrl
                    capObj['name'] = capName

                    caplist.append(capObj)

        bookObj['chapterNum'] = len(caplist)
        # bookAreadyCrawled = insertBook(bookObj)
        # if not bookAreadyCrawled:
        #     checkCapsSql = 'select count(*) from cn_dushu_article where bookId = %d' % (bookObj['id'])
        #     try:
        #         csor.execute(checkCapsSql)
        #         conn.commit()
        #         results = csor.fetchall()
        #
        #         if not results or len(results) < 1:
        #             return None
        #         else:
        #             bookObj['id'] = results[0][0]
        #     except Exception as e:
        #         #     # 发生错误时回滚
        #         print 'check cap count failed ,skip', e

        if not insertBook(bookObj):  # Book信息入库，bookid和uuid写入bookObj
            print 'error, skip, bookName', bookObj['title']
            continue

        bookId = bookObj['id']
        existsCaps = getExistsCaps(bookId)

        for m in range(0, len(caplist)):

            if existsCaps and len(existsCaps) > 0:
                noNeedCrawlCap = False
                for cap in existsCaps:
                    if cap[0] == m and cap[1] > 300:
                        noNeedCrawlCap = True
                        break
                if noNeedCrawlCap:
                    print 'cap exists, no need to recrawl, bookName', bookObj['title'], ' bookId', bookId, ' capIdex: ',m
                    continue

            capUrl = caplist[m]['url']
            capName = caplist[m]['name']
            p.apply_async(insertCap,args=(bookObj, capUrl, capName, m, queue))
            # p.apply(insertCap, (bookObj, capUrl, capName, m))
            # insertCap(bookObj, capUrl, capName)

def updateContentById(id, content):
    # sql = "update cn_dushu_acticle set content = %s where id = %s " % (content, str(id))
    try:
        csor.execute( "update cn_dushu_acticle set content = %s where id = %s ", (content, id))
        conn.commit()
        print id, ' succ cap, ', content[0: 15]
    except Exception as e:
        #     # 发生错误时回滚
        print 'update error ',e
        if conn:
            try:
                conn.rollback()
            except Exception as ee:
                print 'rollback error : '


def updateFromRawUrl():
    ids = '49621,49622,49623,49624,49627,49629'
    for id in ids.split(','):
        updateByBookId(id)


def updateByBookId(id):
    res = getExistsCapsRawUrlId(int(id))
    if not res:
        return
    for cap in res:
        cid = cap[0]
        url = cap[1]
        if not url or len(url) < 1:
            print cid, 'no url, skipp'
            break
        content, host = getAndParse(url)
        if not content:
            continue
        updateContentById(cid, content)

def deleteCapsByBookId(bookId):
    sql = 'delete from cn_dushu_acticle where bookId = %d' % (bookId)
    try:
        csor.execute(sql)
        conn.commit()
        print 'del caps succ'
    except Exception as e:
        #     # 发生错误时回滚
        print 'update error ',e

def selectBooksByCategory(cat):
    sql = 'SELECT id from cn_dushu_book  where typeCode = %d' % (cat);
    try:
        csor.execute(sql)
        conn.commit()
    except Exception as e:
        #     # 发生错误时回滚
        print 'mysql ex: ', e
    results = csor.fetchall()
    return results

def updateByCategoryIdWithUrl(catId):
    res = selectBooksByCategory(catId)
    for book in res:
        bookId = book[0]
        # deleteCapsByBookId(bookId)
        updateByBookId(bookId)

def updateByCategoryIdZongheng(catId):
    sql = 'SELECT id,rawUrl,digest from cn_dushu_book  where categoryCode = ' + str(catId) +' and rawUrl like "%zongheng%" ORDER BY id desc;'
    try:
        csor.execute(sql)
        conn.commit()
    except Exception as e:
        #     # 发生错误时回滚
        print 'mysql ex: ', e
    results = csor.fetchall()
    for book in results:
        bid = book[0]
        url = book[1]
        bookDigest = book[2]
        deleteCapsByBookId(bid)

        url = url.replace('com/book', 'com/showchapter')

        content = getContentWithUA(url, ua)

        soup = getSoupByStr(content)

        caps = soup.select('.chapterBean')
        if not caps:
            continue
        for i in range(0, len(caps)):
            cap = caps[i]
            capUrl = cap.select('a')[0]['href']
            capName = cap.select('a')[0].get_text()
            content, host = getAndParse(capUrl)
            if not content:
                continue
            capObj = dict()
            capObj['title'] = capName
            capObj['rawUrl'] = capUrl
            capObj['source'] = '纵横'
            capObj['content'] = content
            capObj['bookId'] = bid
            capObj['idx'] = i

            m2 = hashlib.md5()
            forDigest = capName + u'#' + str(i)
            # forDigest = u'总裁我很忙#jxj季'
            m2.update(forDigest.encode('utf-8'))
            digest = m2.hexdigest()

            capObj['digest'] = digest
            capObj['size'] = len(content)
            capObj['bookUUID'] = bookDigest

            insertCapWithCapObj(capObj, conn, csor)
            # updateContentById(cid, content)


if __name__ == '__main__':



    updateByCategoryIdZongheng(74)
    # updateFromRawUrl()
    # selectByCategory()
    # from multiprocessing import Pool
    #
    # manager = multiprocessing.Manager()
    # # 父进程创建Queue，并传给各个子进程：
    # queue = manager.Queue(maxsize=20)
    #
    # p = Pool(20)
    #
    # p.apply_async(onlyInsertCap, args=(queue,))
    # #
    # easouCrawl()
    # csor.close()
    # conn.close()


