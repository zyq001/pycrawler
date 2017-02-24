##!/usr/bin/python
# -*- coding: UTF-8 -*-


import json
import time

import requests

from dbHelper import getConn

csor,conn = getConn()

sql = "select * from t_topic_src where id > 1756 and id < 1859"


print sql
csor.execute(sql)

results = csor.fetchall()
for row in results:
    content = row[4]
    # content = row[4].replace('mi', 'mo')
    id = row[0]
    sample = row[13]
    extend = row[14]



    # soup = getSoupByStr(content)
    #
    # for mi in soup.select("mi"):
    #     test = mi.get_text()
    #     if u'\u4e00' <= test <= u'\u9fff' or test == u'\uff08' or test == u'\uff09' :
    #     # if test.startswith(u'&#'):
    #         mi.name = 'mo'
    #         # print test
    # # for math in soup.select("math"):
    # #     length = len(unicode(math))
    # #     if length > 300:
    # #         math['display'] = "block"
    # content = unicode(soup.body).replace(u'<body>','').replace(u'</body>','')

    input = {
    "format": "TeX",
        "svg":"true",
        "mml":"false",
        "renderer":"SVG",
        "png":"false",
        "html":content,
        "speakText": "true",
        "speakRuleset": "mathspeak",
        "speakStyle": "default",
        "ex": 6,
        "width": 50,
        "xmlns": "mml",
        "linebreaks":  { "automatic": "true" }

    }

    start = time.time()

    r = requests.post('http://localhost:8003/', data = json.dumps(input))

    end = time.time()
    print end - start
    content =  r.content

    sampleR = ''
    extendR = ''

    if sample and len(sample) > 0:
        input['width'] = 38
        input['html'] = sample
        r = requests.post('http://localhost:8003/', data = json.dumps(input))
        sampleR = r.content

    if extend and len(extend) > 0:
        input['width'] = 60
        input['html'] = extend
        r = requests.post('http://localhost:8003/', data = json.dumps(input))
        extendR = r.content


    sql = "update t_topic_new set content = '%s',sample = '%s',extend = '%s' where id = %s" % \
          (content, sampleR, extendR, id)
    try:
    #     # 执行sql语句
    #     # print sql
        csor.execute(sql)
    #     # 提交到数据库执行
        print conn.commit()
    except:
    #     # 发生错误时回滚
        conn.rollback()