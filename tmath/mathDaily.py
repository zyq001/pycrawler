#!/usr/bin/python
# -*- coding: UTF-8 -*-
import json
import urlparse

# import Config
from Config import defaultPCUa
from dbHelper import getConn, getTmathConnCsor
from framework.htmlParser import getSoupByStr
from networkHelper import getContent, getContentWithUA

conn = None
csor = None

def juren():
    csor,conn = getConn()

    #小升初笑话
    for i in range(1,25):
        #笑话
        # url = 'http://aoshu.juren.com/chzt/shuxueshouchaobao/xiaoxiaohua/index_' + str(i) + '.html'
        #故事
        url = 'http://aoshu.juren.com/tiku/mryt/yimryt/index_' + str(i) + '.html'
        #名人
        # url =
        if i == 1:
            # url = 'http://aoshu.juren.com/chzt/shuxueshouchaobao/xiaoxiaohua/'
            # url = 'http://aoshu.juren.com/chzt/shuxueshouchaobao/xiaogushi/'
            # url = 'http://aoshu.juren.com/chzt/shuxueshouchaobao/neirongsucai/'
            url = 'http://aoshu.juren.com/tiku/mryt/yimryt/'
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
                for j in range(1,length):
                    pJ = ps[j]
                    pText = pJ.get_text()
                    if u'本期精彩专题推荐' in pText or u'本期' in pText or u'精彩推荐' in pText\
                            or u'点击下一页查看答案' in pText or u'下一页查看答案' in pText or u'查看答案' in pText\
                            or len(pJ.select('a')) > 0:
                        print 'not content,break,  text:' + pText
                        break
                    content += unicode(pJ)
                contentHtml2 = getContent(deatilUrl.replace('.html', '_2.html'))
                if not contentHtml2:
                    print 'get detail failed'
                    continue
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
                      (title, 3, content, '3', 1)
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

def today():
    baseUrl = 'http://www.todayonhistory.com/'
    conn,csor = getTmathConnCsor()
    for month in range(1,13):
        for day in range(1,32):
            type = '全部'
            jsonurl = baseUrl + str(month) + '/' + str(day)
            htmlContent = getContentWithUA(jsonurl, defaultPCUa)
            if not htmlContent or u'404-历史上的今天' in htmlContent:
                print 'no content skip month:',str(month),' day:',str(day)
                continue
            soup = getSoupByStr(htmlContent)
            if '404' in soup.title:
                print '404 skip month:',str(month),' day:',str(day)
                continue
            listUl = soup.select_one('ul.oh')
            for listLi in listUl.select('li'):
                liClasses = listLi['class']
                if 'typeid_53' in liClasses:
                    type = u'纪念'
                elif 'typeid_54' in liClasses:
                    type = u'节假日'
                elif 'typeid_55' in liClasses:
                    type = u'逝世'
                elif 'typeid_56' in liClasses:
                    type = u'出生'
                elif 'typeid_57' in liClasses:
                    type = u'事件'
                solarYear = listLi.select_one('span[class="poh"]').get_text()
                link = listLi.select_one('a')
                if not link:
                    print 'no link content, maybe bs4 bug, skip'
                    continue
                contentUrl = link['href']
                title = link['title']
                contentText = ''

                imgUrl = ''
                imgTag = listLi.select_one('img')
                if imgTag:
                    imgUrl = urlparse.urljoin(baseUrl,imgTag['src'])

                detailContentHtml = getContentWithUA(contentUrl,defaultPCUa)
                if detailContentHtml:
                    contentSoup = getSoupByStr(detailContentHtml)
                    contentBody = contentSoup.select_one('.body')

                    n1 = contentBody.select_one('.page')
                    if  n1:
                        n1.extract()
                    n2 = contentBody.select_one('.keyword')
                    if n2:
                        n2.extract()
                    n3 = contentBody.select_one('.extra')
                    if n3:
                        n3.extract()
                    n4 = contentBody.select_one('.mgg')
                    if n4:
                        n4.extract()
                    n5 = contentBody.select_one('.poh')
                    if n5:
                        n5.extract()
                    n6 = contentBody.select_one('.framebox')
                    if n6:
                        n6.extract()

                    # for divTag in contentBody.select('div'):
                    #     divTag.extract()# 去除多余的div

                    contentText = unicode(contentBody)
                csor.execute('insert ignore into daily_today (name ,type ,content  '
                             ',month ,day ,thumbImg ,solaryear,srcUrl) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
                             , (title,type,contentText,month,day,imgUrl,solarYear,contentUrl))
                conn.commit()

            jsonBaseUrl = 'http://www.todayonhistory.com/index.php?m=content&c=index&a=json_event&page='
            #&pagesize=40&month=2&day=13'
            for page in range(1,5):
                jsonurl = jsonBaseUrl + str(page) + '&pagesize=40&month=' + str(month) + '&day=' + str(day)
                jsonContent = getContentWithUA(jsonurl, defaultPCUa)
                if not jsonContent or len(jsonContent) < 10:
                    print 'json url return null or too short, maybe finished'
                    break
                jsonLists = json.loads(jsonContent)
                for jsonObj in jsonLists:
                    tid = jsonObj['id']
                    contentUrl2 = jsonObj['url']
                    title = jsonObj['title']
                    thumb = urlparse.urljoin(baseUrl,jsonObj['thumb'])
                    solaryear = jsonObj['solaryear']

                    contentText = ''

                    detailContentHtml = getContentWithUA(contentUrl2, defaultPCUa)
                    if detailContentHtml:
                        contentSoup = getSoupByStr(detailContentHtml)
                        contentBody = contentSoup.select_one('.body')
                        # for divTag in contentBody.select('div'):
                        #     divTag.extract()  # 去除多余的div


                        n1 = contentBody.select_one('.page')
                        if n1:
                            n1.extract()
                        n2 = contentBody.select_one('.keyword')
                        if n2:
                            n2.extract()
                        n3 = contentBody.select_one('.extra')
                        if n3:
                            n3.extract()
                        n4 = contentBody.select_one('.mgg')
                        if n4:
                            n4.extract()
                        n5 = contentBody.select_one('.poh')
                        if n5:
                            n5.extract()
                        n6 = contentBody.select_one('.framebox')
                        if n6:
                            n6.extract()
                        n7 = contentBody.select_one('.mad')
                        if n7:
                            n7.extract()

                        contentText = unicode(contentBody)
                    csor.execute('insert ignore into daily_today (name ,type ,content  '
                                 ',month ,day ,thumbImg ,solaryear,srcUrl) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
                                 , (title, '全部', contentText, month, day, thumb, solaryear, contentUrl2))
                    conn.commit()

            print 'done month:',str(month), ' day: ',str(day)

def koolearn(muluUrl, stage):

    global conn,csor
    if not conn or (not csor):
        conn,csor = getTmathConnCsor()

    muluHtmlContent = getContentWithUA(muluUrl,defaultPCUa)
    muluSoup = getSoupByStr(muluHtmlContent)
    for pageLi in muluSoup.select('.list01 ul li'):
        title = pageLi.select_one('h3').get_text()
        if u'下载' in title:
            continue
        descTag = pageLi.select_one('.js2 p')
        if not descTag:
            descTag = pageLi.select_one('.js p')
        desc = descTag.get_text()

        tags = pageLi.select_one('.c_lv')['title']
        ntype = tags
        if len(tags) > 3:
            ts = tags.split(' ')
            if len(ts) > 2:
                ntype = ts[1]
        contentUrl = pageLi.select_one('h3 a')['href']
        detailHtmlContent = getContentWithUA(contentUrl,defaultPCUa)
        detailContentSoup = getSoupByStr(detailHtmlContent)
        detailContent = ''
        contentDiv = detailContentSoup.select_one('.show_l2 .mt40')
        contentDiv.select('p')[0].extract()# 第一个p标签为介绍，删掉
        cps = contentDiv.select('p')
        for ci in range(0,len(cps)):
            if  cps[ci].select('a'):
                print 'has link ,extract, link:',unicode(cps[ci]),' contentUrl:',contentUrl
                cps[ci].extract()

            if ci in [len(cps) - 1, len(cps) - 2, len(cps) - 3] and (u'新东方' in cps[ci].get_text() or
                                                                             u'来源' in cps[ci].get_text()):
                for cc in range(ci, len(cps)):
                    cps[cc].extract()
                break
        detailContent = detailContent + unicode(contentDiv)

        # 如果有分页，不算最后一个回链页
        for page in range(2,6):
            cUrl = contentUrl.replace('.html','_' + str(page) + '.html')
            moreContentHtmlContent = getContentWithUA(cUrl,defaultPCUa)
            if not moreContentHtmlContent:
                print 'no more content, ',cUrl
                break
            moreContentSoup = getSoupByStr(moreContentHtmlContent)
            #去掉最后两个p
            moreContentDiv = moreContentSoup.select_one('.show_l2 .mt40')
            pps = moreContentDiv.select('p')
            for ci in range(0, len(pps)):
                if  pps[ci].select('a') :
                    print 'has link ,extract, link:',unicode(pps[ci]),' contentUrl:',cUrl
                    pps[ci].extract()

                if ci in [len(pps) - 1, len(pps) - 2, len(pps) - 3] and (u'新东方' in pps[ci].get_text() or
                                                                                 u'来源' in pps[ci].get_text()):
                    for cc in range(ci, len(pps)):
                        pps[cc].extract()
                    break
            # pps[len(pps) - 1].extract()
            # pps[len(pps) - 2].extract()
            detailContent = detailContent + unicode(moreContentDiv)

        #入库
        csor.execute('insert ignore into daily_news (name,type,content,stage,author,tag,contentUrl,description) VALUES (%s,'
                     '%s,%s,%s,%s,%s,%s,%s)', (title,ntype, detailContent
                                               .replace(u'新东方在线论坛','').replace(u'相关链接：','').replace(u'来源：新东方在线论坛','')
                                                                    , stage, u'新东方', tags,contentUrl,desc))
        conn.commit()


    #获取下一页
    footLinks = muluSoup.select('#page a')
    nextUrl = footLinks[len(footLinks) - 1]['href']
    koolearn(urlparse.urljoin(muluUrl,nextUrl),stage)





if __name__ == '__main__':
    # juren()
    # today()
    koolearn('http://xiaoxue.koolearn.com/xiaoshengchu/xscfd/shuxue/', u'小学')
    koolearn('http://gaokao.koolearn.com/shuxue/gongshi/', u'高中')
    koolearn('http://zhongkao.koolearn.com/shuxue/zhidao/', u'初中')
    koolearn('http://gaokao.koolearn.com/shuxue/zhidao/', u'高中')
    koolearn('http://gaokao.koolearn.com/shuxue/yazhouti/', u'高中')