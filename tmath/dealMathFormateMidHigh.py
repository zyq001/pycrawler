##!/usr/bin/python
# -*- coding: UTF-8 -*-
import bs4

from dbHelper import getConn
from framework.htmlParser import getSoupByStr

csor,conn = getConn()

# sql = "select * from t_topic_new where id = 2645 or id = 2678"
# sql = "select * from t_topic_new where id > 2994 and id < 3094"
# sql = "select * from t_topic_new where id > 3411 and  id < 3730"#初中
sql = "select * from t_topic_new where id > 1647 and  id < 1757"#大学


print sql
csor.execute(sql)

results = csor.fetchall()
for row in results:
    content = row[4]
    # content = row[4].replace('mi', 'mo')
    id = row[0]



    soup = getSoupByStr(content)

    t = soup.select('.title')
    if t and len(t) == 1:
        t[0].extract()
    else:
        print 'exteact Title fail, title:', unicode(t).encode('utf-8')

    pointStr = u""
    sampleStr = u""



    sampleBgn = False
    answerBgn = False
    answerEnd = False#多个例题时碰到 需要标记解析何时结束

    answerTag = soup.new_tag('div')
    answerTag['class'] = 'answrapper'
    answerTag['id'] = 'answrapper'
    answerTag['style'] = 'display:none'

    parents = soup.body.contents
    if len(parents) == 1:
        parents = parents[0].contents

    for text in parents:
        if isinstance(text, bs4.element.NavigableString):
            t = unicode(text)
        else:
            t = text.get_text()
        if u'〖实例〗' in t:
            sampleBgn = True
            continue
        elif u'实例' in t:
            print '*********不标准？****', t
            sampleBgn = True
            continue
        # elif u'分析：' in t or u'解答：' in t or u'解：' in t:
        #     print '*********解开始****', t
        #     answerBgn = True
        #
        # elif u'例2' in t or u'例：' in t or u'例3：' in t or u'例4：' in t:
        #     print '*********解结束，有一个例题开始****', t
        #     answerEnd = True
        #     answerBgn = False
        #
        #     sampleStr = sampleStr + unicode(answerTag)
        #
        #     answerTag = soup.new_tag('div')
        #     answerTag['class'] = 'answrapper'
        #     answerTag['id'] = 'answrapper'
        #     answerTag['style'] = 'display:none'


        if sampleBgn:
            sampleStr = sampleStr + unicode(text)

        # elif answerBgn:
        #     answerTag

        else:
            pointStr = pointStr + unicode(text)


    pointStr = '<p>' + pointStr + '</p>'
    pointStr = pointStr.replace('<br/>', '</p><p>').replace('<br>','</p><p>').replace('<p></p>', '')

    sampleStr = '<p>' + sampleStr + '</p>'
    sampleStr = sampleStr.replace('<br/>', '</p><p>').replace('<br>','</p><p>').replace('<p></p>', '')



    # for br in soup.body.br:
    #     br.extract()
        # ptag = soup.new_tag('p')
        # text.wrap(ptag)



    #扩展知识
    extendStr = u""

    # content = unicode(soup.body).replace(u'<body>','').replace(u'</body>','')

    content = pointStr
    content = content.replace('\\', r'\\')
    content = content.replace('\'', r'\'')
    sampleStr = sampleStr.replace('\\', r'\\')
    sampleStr = sampleStr.replace('\'', r'\'')
    # extendStr = extendStr.replace('\\', r'\\')


    sql = "update t_topic_new set content = '%s',sample = '%s', extend = '%s' where id = %s" % \
          (content,sampleStr, extendStr, id)
    try:
    #     # 执行sql语句
    #     # print sql
        csor.execute(sql)
    #     # 提交到数据库执行
        print conn.commit()
    except:
    #     # 发生错误时回滚
        conn.rollback()