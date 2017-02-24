##!/usr/bin/python
# -*- coding: UTF-8 -*-


import time

from readability import Document

from dbHelper import getDushuConn
from networkHelper import getContent

csor,conn = getDushuConn()

id = 825689

while id > 0:

    sql = "select id,rawUrl from cn_dushu_acticle where id > %d and id < %s and bookId != 49316 ORDER by id desc" % (id - 500, id)

    print sql
    csor.execute(sql)

    time.sleep(2)

    id = id - 500;

    results = csor.fetchall()

    lastTime = 0

    for row in results:
        # content = row[1]
        # content = row[4].replace('mi', 'mo')
        id = row[0]
        # url = row[1]
        url = 'http://www.3dllc.com/html/37/37023/9515879.html'

        # if not u'easou' in url:
        #     continue

        newContent = getContent(url)

        doc = Document(newContent)

        content = doc.summary(html_partial=True)

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
        content = content.replace('\'', r'\'')



        sql = "update cn_dushu_acticle set content = '%s' where id = %s" % \
              (content, id)

        now = time.time()

        if now - lastTime < 5 * 1000:
            time.sleep(5)

        try:
        #     # 执行sql语句
        #     # print sql
        #     csor.execute(sql)
            lastTime = time.time()

        #     # 提交到数据库执行
            print conn.commit()
        except Exception as e:
        #     # 发生错误时回滚
            print e
            conn.rollback()

csor.close()