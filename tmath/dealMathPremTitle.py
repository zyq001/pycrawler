##!/usr/bin/python
# -*- coding: UTF-8 -*-


import re

from dbHelper import getConn

csor,conn = getConn()

sql = "select id,content from t_topic_new where id > 1647 and  id < 1757"


print sql
csor.execute(sql)

results = csor.fetchall()
for row in results:
    content = row[1]
    # content = row[4].replace('mi', 'mo')
    id = row[0]



    # soup = getSoupByStr(content)
    #
    # t = soup.select('p')[0]
    # title = t.get_text()
    # if re.match('\d+.*',title):
    #     # if id < 1766:
    #     #     t.b.string = title[1:]
    #     # else:
    #     #     t.b.string = title[2:]
    #     t.extract()




    # content = unicode(soup.body).replace(u'<body>','').replace(u'</body>','')

    content = content.replace('\n', '')
    # content = content.replace(r'<p>\d+、.*</b></p>', '')

    content = re.sub(u'<p>\d+、((?:.|\n)*?)</p>', "", content, 1)


    sql = "update t_topic_new set content = '%s' where id = %s" % \
          (content, id)
    try:
    #     # 执行sql语句
    #     # print sql
        csor.execute(sql)
    #     # 提交到数据库执行
        print conn.commit()
    except:
    #     # 发生错误时回滚
        conn.rollback()