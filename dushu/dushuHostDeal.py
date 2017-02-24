##!/usr/bin/python
# -*- coding: UTF-8 -*-


import time

from dbHelper import getDushuConn
from framework.htmlParser import getSoupByStr
from networkHelper import getContentAndRedictedUrl

csor,conn = getDushuConn()

id = 828183

hostsCount = {}

while id > 0:

    sql = "select rawUrl from cn_dushu_book"

    print sql
    csor.execute(sql)

    time.sleep(2)

    # id = id - 5000;
    id = -1

    results = csor.fetchall()

    lastTime = 0

    for row in results:
        # content = row[1]
        # content = row[4].replace('mi', 'mo')
        # id = row[0]
        url = row[0]
        # url = 'http://www.3dllc.com/html/37/37023/9515879.html'

        if not u'easou' in url:
            continue

        newContent,redictedUrl = getContentAndRedictedUrl(url)

        if not newContent:
            continue
        if not redictedUrl:
            continue

        soup = getSoupByStr(newContent)



        for src in soup.select(".source"):
            host = src.get_text()
            count = 1
            if hostsCount.has_key(host):
                count = hostsCount[host] + 1
            hostsCount[host] = count

        # host = urlparse(redictedUrl).hostname
        # count = 1
        # if hostsCount.has_key(host):
        #     count = hostsCount[host] + 1
        # hostsCount[host] = count

        #
        # soup = getSoupByStr(newContent)
        #
        # ps = soup.select('#chapterContent')[0]
        # # ps.select('div')[0].unwrap()
        # # ps.unwrap()
        # for water in soup.select('.watermark'):
        #     water.extract()

        #
        # t = soup.select('p')[0]
        # title = t.get_text()
        # if re.match('\d+.*',title):
        #     # if id < 1766:
        #     #     t.b.string = title[1:]
        #     # else:
        #     #     t.b.string = title[2:]
        #     t.extract()


        # del ps.attrs

        # content = unicode(ps).replace(u'<body>','').replace(u'</body>','').replace(u'<div id="chapterContent" class="content">','')\
        #     .replace(u'</div>', '').replace(u'<div class="content" id="chapterContent">', '').replace('<div>','')

        # newSoup = getSoupByStr(content)
        # newSoup.select('div')[0].unwrap()


        # content = unicode(newSoup).replace(u'<body>','').replace(u'</body>','')
        # content = content.replace(r'<p>\d+、.*</b></p>', '')

        # content = re.sub(u'<p>\d+、((?:.|\n)*?)</p>', "", content, 1)
        # content = content.replace('\'', r'\'')
        #
        #
        #
        # sql = "update cn_dushu_acticle set content = '%s' where id = %s" % \
        #       (content, id)
        #
        # now = time.time()
        #
        # if now - lastTime < 5 * 1000:
        #     time.sleep(5)
        #
        # try:
        # #     # 执行sql语句
        # #     # print sql
        # #     csor.execute(sql)
        #     lastTime = time.time()
        #
        # #     # 提交到数据库执行
        #     print conn.commit()
        # except Exception as e:
        # #     # 发生错误时回滚
        #     print e
        #     conn.rollback()

csor.close()


sites = open(u'hostCount.txt', 'a')

sites.write(hostsCount)
sites.flush()
sites.close()