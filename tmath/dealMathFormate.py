##!/usr/bin/python
# -*- coding: UTF-8 -*-


from dbHelper import getConn
from framework.htmlParser import getSoupByStr

csor,conn = getConn()

sql = "select * from t_topic_src where id > 1756 and id < 1859"


print sql
csor.execute(sql)

results = csor.fetchall()
for row in results:
    content = row[4]
    # content = row[4].replace('mi', 'mo')
    id = row[0]



    soup = getSoupByStr(content)

    t = soup.select('p')[0]
    t['style'] = 'display:none'
    # title = t.get_text()

    sampleStr = u""

    for sampleName in soup.select('.sampleName'):

    # if len(sampleNames) >0 :
        sampleName.extract()



    samples = soup.select(".sample")
    showbtns = soup.select(".showbtn")
    answrappers = soup.select(".answrapper")
    for i in range(0, len(samples)):
        sampleStr = sampleStr + unicode(samples[i].extract())
        if i < len(showbtns):
            sampleStr = sampleStr + unicode(showbtns[i].extract())
        if i < len(answrappers):
            sampleStr = sampleStr + unicode(answrappers[i].extract())

    # for sampleDiv in soup.select(".sample"):
    # # if len(sampleDiv) >0 :
    #     sampleStr = sampleStr + unicode(sampleDiv.extract())
    #
    # for showbtn in soup.select(".showbtn"):
    # # if len(showbtn) >0 :
    #     sampleStr = sampleStr + unicode(showbtn.extract())
    #
    #
    # for answrapper in soup.select(".answrapper"):
    # # if len(answrapper) >0 :
    #     sampleStr = sampleStr + unicode(answrapper.extract())




    #扩展知识
    extendStr = u""

    for extendName in soup.select('.extendName'):
    # if len(extendName) >0 :
        extendName.extract()

    for extendwrapper in soup.select(".extendwrapper"):
    # if len(extendwrapper) >0 :
        extendStr = extendStr + unicode(extendwrapper.extract())



    content = unicode(soup.body).replace(u'<body>','').replace(u'</body>','')

    content = content.replace('\\', r'\\')
    sampleStr = sampleStr.replace('\\', r'\\')
    extendStr = extendStr.replace('\\', r'\\')


    sql = "update t_topic_src set content = '%s',sample = '%s', extend = '%s' where id = %s" % \
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