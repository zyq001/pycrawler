##!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys

from bs4 import Comment

reload(sys)
sys.setdefaultencoding( "utf-8" )

from urlparse import urlparse

import requests

from readability import Document

from dbHelper import getDushuConn
from config import loadRules, loadIgnores
from htmlParser import getSoupByStrEncode
import time
import re

from networkHelper import getContentAndRedictedUrl, getRedictedUrl

id = 823737

lastTime = 0

# sites.readline()
#
# from pybloom import BloomFilter
# f = BloomFilter(capacity=1000, error_rate=0.001)
# [f.add(x) for x in range(10)]

rules = loadRules()
ignores = loadIgnores()

def update(csor, conn, id,host, content):
    # sql = "update cn_dushu_acticle set content = '%s',source = '%s' where id = %s" % \
    #       (content.encode('utf-8'),host, id)

    now = time.time()

    global lastTime

    if now - lastTime < 5 * 1000:
        time.sleep(5)

    try:
        #     # 执行sql语句
        #     # print sql
        csor.execute("update cn_dushu_acticle set content = %s,source = %s where id = %s" , \
          (content.encode('utf-8'),host, id))
        lastTime = time.time()

        #     # 提交到数据库执行
        # print \
        conn.commit()
    except Exception as e:
        #     # 发生错误时回滚
        # print 'update rollback',e
        conn.rollback()

def removeNodesFromSoup(rule, soup):
    for r in soup.select(rule):
        if r:
            r.extract()


def cleanTailHead(urlhost, content):
    # rules = loadRules()
    if not rules.has_key(urlhost):
        return content
    host = rules[urlhost]
    head = host['head']
    if head and len(head) > 0:
        index = content.find(head)
        if index > 0:
            content = content[index + len(head):]
    tail = host['tail']
    if tail and len(tail) > 0:
        index = content.find(tail)
        if index > 0:
            content = content[:index]

    noise = host['noise']
    if noise and len(noise) > 0:
        for n in noise:
            content = re.sub(n, "", content)

    #清理通用噪声
    common = rules['common']
    head = common['head']
    if head and len(head) > 0:
        index = content.find(head)
        if index > 0:
            content = content[index + len(head):]
    tails = common['tail']
    if tails and len(tails) > 0:
        for tail in tails:
            index2 = content.find(tail)
            if index2 > 0:
                content = content[:index2]

    noise = common['noise']
    if noise and len(noise) > 0:
        for n in noise:
            content = re.sub(n, "", content)

    #清理多余的回车和换行
    content = content.replace(u'\n\n', u'\n').replace(u'<br><br>', u'<br>').replace(u'<br\><br\>', u'<br\>')

    return content

def getAndParse(url):
    # 跳过的host
    continue1 = False

    redUrl = getRedictedUrl(url)

    for ig in ignores['hosts']:
        if ig in url or (redUrl and ig in redUrl):
            continue1 = True
            break
    if continue1:
        return None,None


    try:
        newContent, redUrl = getContentAndRedictedUrl(url)

    except Exception as e:
        print 'new content1', e
        try:
            newContent, redUrl = getContentAndRedictedUrl(url)

        except Exception as e:
            print 'new content1', e
            return None,None

        except requests.exceptions.ConnectionError as er:
            print 'new content2', er
            return None,None

    except requests.exceptions.ConnectionError as er:
        print 'new content2', er
        try:
            newContent, redUrl = getContentAndRedictedUrl(url)

        except Exception as e:
            print 'new content1', e
            return None,None

        except requests.exceptions.ConnectionError as er:
            print 'new content2', er
            return None,None

    if not redUrl:
        return None,None


    # 对跳转后的url，再过滤一遍
    # continue2 = False
    # for ig in ignores['hosts']:
    #     if ig in redUrl:
    #         continue2 = True
    #         return None
    #
    # if continue2:
    #     return None


    urlHost = urlparse(redUrl).hostname

    new2 = newContent.encode('utf-8')
    # soup = getSoupByStr(newContent)
    soup = getSoupByStrEncode(new2, "utf-8")

    # 统一清理通用rm

    for rm in rules['common']['rm']:
        removeNodesFromSoup(rm, soup)  # 删除停止node

    needAutoExtract = True

    if rules.has_key(urlHost):
        contentRule = rules[urlHost]['content']
        if contentRule:  # 有配置正文规则
            specContent = soup.select(contentRule)  # 根据配置，抽取正文
            if specContent and len(specContent) > 0:
                del specContent[0].attrs
                soup = specContent[0]
                needAutoExtract = False
                # 现规则多直接按标签删除，所有，只有找到content才清楚rm配置的选项
                if rules[urlHost]['rm'] and len(rules[urlHost]['rm']) > 0:
                    for rm in rules[urlHost]['rm']:
                        removeNodesFromSoup(rm, soup)  # 删除停止node

        unwrapUseless(soup)



        content = unicode(soup).replace(u'<body>', '').replace(u'</body>', '') \
            .replace(u'</div>', '').replace(u'<div>', '')

    else:  # m没有配置任何规则，自动抽取正文
        # print urlHost, ' : ',url
        # return None

        attemp = soup.select('#content') #很多小说网站正文都是#content
        if attemp and len(attemp):
            #猜中了
            needAutoExtract = False
            unwrapUseless(soup)
            content = unicode(soup).replace(u'<body>', '').replace(u'</body>', '') \
                .replace(u'</div>', '').replace(u'<div>', '')
        # else:
    if needAutoExtract:
        unwrapUseless(soup)

        doc = Document(unicode(soup).encode('utf-8'))  #可能会报这个错误Expected a bytes object, not a unicode object
        content = doc.summary(html_partial=True)
        # content = content.replace('<html>','').replace('</html>','')

    newContent2 = cleanTailHead(urlHost, content)
    if newContent2 != content:
        content = newContent2

    if content and len(content) < 10:
        return None,None

    # newSoup = getSoupByStr(content)
    # newSoup.select('div')[0].unwrap()

    # content = unicode(newSoup).replace(u'<body>','').replace(u'</body>','')
    # content = content.replace(r'<p>\d+、.*</b></p>', '')

    # content = re.sub(u'<p>\d+、((?:.|\n)*?)</p>', "", content, 1)
    content = content.replace(u'�', u'')
    content = content.replace(u'\'', r'\'')
    return content,urlHost


def unwrapUseless(soup):
    # unwrap常见无用标签
    for a in soup.select('a'):
        a.unwrap()
    for a in soup.select('b'):
        a.unwrap()
    for a in soup.select('font'):
        a.unwrap()
    for a in soup.select('span'):
        a.unwrap()
    comments = soup.findAll(text=lambda text: isinstance(text, Comment))
    [comment.extract() for comment in comments]


def recrawl():
    csor, conn = getDushuConn()

    while id > 0:

        sql = "select id,rawUrl,content from cn_dushu_acticle where id > %d and id < %s and bookId != 49316 ORDER by id desc" % (id - 1000, id)



        print 'begin'
        csor.execute(sql)

        time.sleep(2)

        id = id - 1000

        results = csor.fetchall()


        for row in results:
            content = row[2]
            # content = row[4].replace('mi', 'mo')
            id = row[0]
            url = row[1]
            # url = 'http://b.easou.com/w/read/85356/14075857/6.html'

            # #跳过的host
            # continue1 = False
            # for ig in ignores['hosts']:
            #     if ig in url:
            #         continue1 = True
            #         continue
            # if continue1:
            #     continue
            #
            # try:
            #     newContent,redUrl = getContentAndRedictedUrl(url)
            #
            # except Exception as e:
            #     print 'new content1',e
            #     try:
            #         newContent, redUrl = getContentAndRedictedUrl(url)
            #
            #     except Exception as e:
            #         print 'new content1', e
            #         continue
            #     except requests.exceptions.ConnectionError as er:
            #         print 'new content2', er
            #         continue
            # except requests.exceptions.ConnectionError as er:
            #     print 'new content2',er
            #     try:
            #         newContent, redUrl = getContentAndRedictedUrl(url)
            #
            #     except Exception as e:
            #         print 'new content1', e
            #         continue
            #     except requests.exceptions.ConnectionError as er:
            #         print 'new content2', er
            #         continue
            #
            # if not redUrl:
            #     continue
            #
            # #对跳转后的url，再过滤一遍
            # continue2 = False
            # for ig in ignores['hosts']:
            #     if ig in redUrl:
            #         continue2 = True
            #         continue
            # if continue2:
            #     continue
            #
            # urlHost = urlparse(redUrl).hostname
            #
            # new2 = newContent.encode('utf-8')
            # soup = getSoupByStr(new2, "utf-8")
            #
            #
            #
            # #统一清理通用噪声
            #
            # for rm in rules['common']['rm']:
            #     removeNodesFromSoup(rm, soup)  # 删除停止node
            #
            #
            # if rules.has_key(urlHost):
            #     contentRule = rules[urlHost]['content']
            #     if contentRule: #有配置正文规则
            #         specContent = soup.select(contentRule)#根据配置，抽取正文
            #         if specContent and len(specContent) > 0:
            #             del specContent[0].attrs
            #             soup = specContent[0]
            #     #不管有没有配置正文规则，都应该遍历删除rm节点
            #     if rules[urlHost]['rm'] and len(rules[urlHost]['rm']) > 0:
            #         for rm in rules[urlHost]['rm']:
            #             removeNodesFromSoup(rm, soup)#删除停止node
            #
            #     # unwrap常见无用标签
            #     for a in soup.select('a'):
            #         a.unwrap()
            #     for a in soup.select('b'):
            #         a.unwrap()
            #     for a in soup.select('font'):
            #         a.unwrap()
            #     for a in soup.select('span'):
            #         a.unwrap()
            #
            #     content = unicode(soup).replace(u'<body>', '').replace(u'</body>', '') \
            #         .replace(u'</div>', '').replace(u'<div>', '')
            #
            # else:  #m没有配置任何规则，自动抽取正文
            #     print urlHost,' : ',id
            #     continue
            #     # doc = Document(unicode(soup))
            #     # content = doc.summary(html_partial=True)
            #     # urlContents[url] = content.encode('utf-8')
            #
            #
            #
            # newContent2 = cleanTailHead(urlHost, content)
            # if newContent2 != content:
            #     content = newContent2
            #
            # if content and  len(content) < 10:
            #     continue
            #
            # # newSoup = getSoupByStr(content)
            # # newSoup.select('div')[0].unwrap()
            #
            # # content = unicode(newSoup).replace(u'<body>','').replace(u'</body>','')
            # # content = content.replace(r'<p>\d+、.*</b></p>', '')
            #
            # # content = re.sub(u'<p>\d+、((?:.|\n)*?)</p>', "", content, 1)
            # content = content.replace(u'�', u'')
            # content = content.replace(u'\'', r'\'')

            content, urlHost = getAndParse(url)

            if content and urlHost:
                update(csor, conn, id,urlHost, unicode(content))
    csor.close()


if __name__ == '__main__':
    # update()
    redUrl = getRedictedUrl('http://b.easou.com/w/read/9109979/12810973/1.html')
    print redUrl

