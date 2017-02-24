#!/usr/bin/python
# -*- coding: UTF-8 -*-

from dbHelper import getConn
from framework.htmlParser import getSoupByStr
from networkHelper import getContent


def juren():
    csor,conn = getConn()

    urls = ['yi','er','san','si','wu','liu']

    for m in range(1, 7):
        for i in range(1,25):
            print 'i:',i,'    m: ',m
            #笑话
            # url = 'http://aoshu.juren.com/chzt/shuxueshouchaobao/xiaoxiaohua/index_' + str(i) + '.html'
            #故事
            url = 'http://aoshu.juren.com/tiku/mryt/' + urls[m - 1] + 'mryt/index_' + str(i) + '.html'
            #名人
            # url =
            if i == 1:
                # url = 'http://aoshu.juren.com/chzt/shuxueshouchaobao/xiaoxiaohua/'
                # url = 'http://aoshu.juren.com/chzt/shuxueshouchaobao/xiaogushi/'
                # url = 'http://aoshu.juren.com/chzt/shuxueshouchaobao/neirongsucai/'
                url = 'http://aoshu.juren.com/tiku/mryt/' + urls[m -1] +'mryt/'
                # url = 'http://aoshu.juren.com/tiku/mryt/liumryt/index_22.html'
            content = getContent(url)
            if not content:
                print 'get content failed, url: ',url
                continue
            soup = getSoupByStr(content)
            if not soup:
                print 'get soup filed, url:', url
                continue
            for listting in soup.select(".listing1"):
                for a in listting.select('a'):
                    text = a.get_text()
                    titles = text.split(u'：')
                    if len(titles) < 2:
                        titles = text.split(u':')
                    if len(titles) < 2:
                        title = text
                    else:
                        title = titles[1]
                    deatilUrl = a['href']
                    contentHtml = getContent(deatilUrl)
                    if not contentHtml:
                        print 'get detail failed'
                        continue
                    contentSoup = getSoupByStr(contentHtml).select('.mainContent')
                    content = ''
                    ps = contentSoup[0].select('p')
                    length = len(ps)
                    for j in range(0,length):
                        pJ = ps[j]
                        pText = pJ.get_text()
                        if u'导语' in pText or u'编者' in pText\
                                or len(pJ.select('a')) > 0:
                            print 'not content,break,  text:' + pText
                            continue
                        if u'本期精彩专题推荐' in pText or u'本期' in pText or u'精彩推荐' in pText\
                                or u'点击下一页查看答案' in pText or u'下一页查看答案' in pText or u'查看答案' in pText\
                                or len(pJ.select('a')) > 0:
                            print 'not content,break,  text:' + pText
                            break
                        content += unicode(pJ)
                    contentHtml2 = getContent(deatilUrl.replace('.html', '_2.html'))
                    if contentHtml2:
                        print 'get detail failed'
                        # continue
                    # contentSoup2 = getSoupByStr(contentHtml2.replace('<br /></p>','')).select('.mainContent')
                        contentSoup2 = getSoupByStr(contentHtml2).select('.mainContent')
                        ps = contentSoup2[0].select('p')
                        length = len(ps)
                        for j in range(0,length):
                            pJ = ps[j]
                            pText = pJ.get_text()
                            if u'本期精彩专题推荐' in pText or u'本期' in pText or u'精彩推荐' in pText or len(pJ.select('a')) > 0:
                                print 'not content,break,  text:' + pText
                                break
                            content += unicode(pJ)

                    sql = "INSERT ignore INTO daily(name, \
                                            type, content,stage, gred) \
                                            VALUES ('%s', '%d', '%s', '%s', '%d')" % \
                          (title, 3, content, '3', m)
                    try:
                        # 执行sql语句
                        print sql
                        csor.execute(sql)
                        # 提交到数据库执行
                        print conn.commit()
                    except:
                        # 发生错误时回滚
                        conn.rollback()
    conn.close()

if __name__ == '__main__':
    juren()