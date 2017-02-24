##!/usr/bin/python
# -*- coding: UTF-8 -*-


import time

import requests

from dbHelper import getDushuConn
from framework.htmlParser import getSoupByStr
from networkHelper import getContentAndRedictedUrl

csor,conn = getDushuConn()

id = 825650

lastTime = 0

sites = open(u'3dWebsite.txt', 'w')

urlContents = {}

# sites.readline()
#
# from pybloom import BloomFilter
# f = BloomFilter(capacity=1000, error_rate=0.001)
# [f.add(x) for x in range(10)]

def update(id, content):
    sql = "update cn_dushu_acticle set content = '%s' where id = %s" % \
          (content, id)

    now = time.time()

    global lastTime

    if now - lastTime < 3 * 1000:
        time.sleep(3)

    try:
        #     # 执行sql语句
        #     # print sql
        csor.execute(sql)
        lastTime = time.time()

        #     # 提交到数据库执行
        print conn.commit()
    except Exception as e:
        #     # 发生错误时回滚
        print e
        conn.rollback()


while id > 0:

    sql = "select id,rawUrl,content from cn_dushu_acticle where id > %d and id < %s and bookId != 49316 ORDER by id desc" % (id - 1000, id)

    if len(urlContents) > 0:
        sites.write(urlContents)
        sites.flush()

    print sql
    csor.execute(sql)

    time.sleep(2)

    id = id - 1000;

    results = csor.fetchall()


    for row in results:
        content = row[2]
        # content = row[4].replace('mi', 'mo')
        id = row[0]
        url = row[1]
        # url = 'http://b.easou.com/w/read/85356/14075857/6.html'

        if u'吉林小说网' in content :
            index = content.find(u'吉林小说网')
            content = content[0:index]
            update(id, content)
            continue

        if u'谢谢79免费小说阅读' in content :
            index = content.find(u'谢谢79免费小说阅读')
            content = content[0:index]
            update(id, content)
            continue


        if not (u'easou' in url or u'3dllc' in url):
            continue
        try:
            newContent,redUrl = getContentAndRedictedUrl(url)

        except Exception as e:
            print e
            continue
        except requests.exceptions.ConnectionError as er:
            print er
            continue
        if not (redUrl and  u'3dllc' in redUrl):
            urlContents[url] = content.encode('utf-8')
            continue

        soup = getSoupByStr(newContent)

        ps = soup.select('.zhang-txt-nei-rong')[0]

        for ad in ps.select('.con_ad'):
            ad.extract()

        for ad in ps.select('.bd-load-s'):
            ad.extract()


        for ad in ps.select('#pageselect'):
            ad.extract()


        for ad in ps.select('iframe'):
            ad.extract()

        for ad in ps.select('script'):
            ad.extract()
        for ad in ps.select('.biao-qian-love-bd'):
            ad.extract()
        for ad in ps.select('.b-g-w'):
            ad.extract()
        for ad in ps.select('div'):
            ad.extract()


        #
        # t = soup.select('p')[0]
        # title = t.get_text()
        # if re.match('\d+.*',title):
        #     # if id < 1766:
        #     #     t.b.string = title[1:]
        #     # else:
        #     #     t.b.string = title[2:]
        #     t.extract()


        del ps.attrs

        content = unicode(ps).replace(u'<body>','').replace(u'</body>','')\
            .replace(u'</div>', '').replace(u'<div>','')

        # newSoup = getSoupByStr(content)
        # newSoup.select('div')[0].unwrap()


        # content = unicode(newSoup).replace(u'<body>','').replace(u'</body>','')
        # content = content.replace(r'<p>\d+、.*</b></p>', '')

        # content = re.sub(u'<p>\d+、((?:.|\n)*?)</p>', "", content, 1)
        content = content.replace('\'', r'\'')

        update(id, content)


sites.write(urlContents)
sites.flush()
sites.close()
csor.close()