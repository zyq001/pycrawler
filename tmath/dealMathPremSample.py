##!/usr/bin/python
# -*- coding: UTF-8 -*-


import re

from dbHelper import getConn
from framework.htmlParser import getSoupByStr

csor,conn = getConn()

sql = "select * from t_topic_src_sample where id = 1848"


print sql
csor.execute(sql)

results = csor.fetchall()
for row in results:
    content = row[4]
    # content = row[4].replace('mi', 'mo')
    id = row[0]



    soup = getSoupByStr(content)

    sampleBegin = False
    ansBegin = False
    extendBegin = False

    sampleDiv = soup.new_tag('div')
    sampleDiv['class'] = 'sample'

    answerDiv = soup.new_tag('div')
    answerDiv['class'] = 'answer'

    extendDiv = soup.new_tag('div')
    extendDiv['class'] = 'extend'


    sampleNameTag = soup.new_tag("p")
    sampleNameTag['class'] = 'sampleName'
    sampleNameTag.string = "例题："
    # sampleDiv.contents.append(sampleNameTag)

    answerNameTag = soup.new_tag("p")
    answerNameTag['class'] = 'answerName'
    answerNameTag.string = "【解析】"

    extendNameTag = soup.new_tag("p")
    extendNameTag['class'] = 'extendName'
    extendNameTag.string = "【扩展知识】"
    # answerDiv.contents.append(answerNameTag)


    t = soup.select('p')[0]
    title = t.get_text()
    if re.match('\d+.*',title):
        if id < 1766:
            t.b.string = title[1:]
        else:
            t.b.string = title[2:]



    for child in soup.body.children:
        test = child.get_text()

        if test.startswith(u'扩展知识'):
            child = child.extract()
            extendBegin = True
            continue

        if extendBegin:
            child = child.extract()
            extendBegin = True
            extendDiv.contents.append(child)
            continue



        if ansBegin or test.startswith(u'解'):
            child = child.extract()
            ansBegin = True
            answerDiv.contents.append(child)
            continue

        if sampleBegin or test.startswith(u'例'):
            sampleBegin = True
            child = child.extract()
            child.string = test.replace(u'例', '',1)
            sampleDiv.contents.append(child)

    if sampleBegin:
        soup.body.contents.append(sampleNameTag)
        soup.body.contents.append(sampleDiv)

    showbtn = soup.new_tag('div')
    showbtn['class'] = 'showbtn'
    showbtn['id'] = 'showbtn'
    showbtn['onclick'] = 'showans()'

    showbtnT = soup.new_tag('p')
    showbtnT.string = '查看解析'
    showbtn.contents.append(showbtnT)


    if ansBegin:
        soup.body.contents.append(showbtn)


    answrapper = soup.new_tag('div')
    answrapper['class'] = 'answrapper'
    answrapper['id'] = 'answrapper'
    answrapper.contents.append(answerNameTag)
    answrapper.contents.append(answerDiv)

    if ansBegin:
        soup.body.contents.append(answrapper)

    extendwrapper = soup.new_tag('div')
    extendwrapper['class'] = 'extendwrapper'
    extendwrapper['id'] = 'extendwrapper'
    extendwrapper.contents.append(extendNameTag)
    extendwrapper.contents.append(extendDiv)

    # soup.body.contents.append(answrapper)

    if extendBegin:
        soup.body.contents.append(extendwrapper)




    content = unicode(soup.body).replace(u'<body>','').replace(u'</body>','')

    content = content.replace('\\', r'\\')

    sql = "update t_topic_src set content = '%s' where id = %s" % \
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