##!/usr/bin/python
# -*- coding: UTF-8 -*-


import time

from dbHelper import getDushuConn
from framework.htmlParser import getSoupByStr
from networkHelper import getContent

csor,conn = getDushuConn()

id = 139839

while id > 0:

    sql = "select id,rawUrl from cn_dushu_acticle where id > %d and id < %s and bookId != 49316 ORDER by id desc" % (id - 5000, id)

    print sql
    csor.execute(sql)

    time.sleep(5)

    id = id - 5000;

    results = csor.fetchall()

    lastTime = 0

    for row in results:
        # content = row[1]
        # content = row[4].replace('mi', 'mo')
        id = row[0]
        url = row[1]

        if not u'zongheng' in url:
            continue

        try:
            newContent = getContent(url)

            soup = getSoupByStr(newContent)

        except Exception as e:
            print e
            continue

        pss = soup.select('#chapterContent')
        if pss and len(pss) > 0:
            ps = pss[0]
        # ps.select('div')[0].unwrap()
        # ps.unwrap()
        for water in soup.select('.watermark'):
            water.extract()

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

        content = unicode(ps).replace(u'<body>','').replace(u'</body>','').replace(u'<div id="chapterContent" class="content">','')\
            .replace(u'</div>', '').replace(u'<div class="content" id="chapterContent">', '').replace('<div>','')

        # newSoup = getSoupByStr(content)
        # newSoup.select('div')[0].unwrap()


        # content = unicode(newSoup).replace(u'<body>','').replace(u'</body>','')
        # content = content.replace(r'<p>\d+、.*</b></p>', '')

        # content = re.sub(u'<p>\d+、((?:.|\n)*?)</p>', "", content, 1)
        content = content.replace('\'', r'\'')



        sql = "update cn_dushu_acticle set content = '%s' where id = %s" % \
              (content, id)

        now = time.time()

        if now - lastTime < 5 * 1000:
            time.sleep(5)

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

csor.close()