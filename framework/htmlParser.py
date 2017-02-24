#!/usr/bin/python
# -*- coding: UTF-8 -*-

from bs4 import BeautifulSoup,Comment
import os
# import html5lib

def getSoupByStr(content):
    soup = BeautifulSoup(content, 'lxml')
    if soup:
        return soup

def getSoupByStrEncode(content, encoding='utf-8'):
    soup = BeautifulSoup(content, 'lxml', from_encoding=encoding)
    if soup:
        return soup


def getSoupByFile(file):
    soup = BeautifulSoup(open(file), 'lxml')
    if soup:
        return soup



def dealLocalFile():
    rootDir = os.getcwd()

    list_dirs = os.walk(rootDir)
    for root, dirs, files in list_dirs:
        # for d in dirs:
        #     print os.path.join(root, d)
        for f in files:
            if f.endswith('html'):
                path = os.path.join(root, f)
                soup = BeautifulSoup(open(path), 'html.parser')
                soup = soup.body

                #去掉注释
                comments = soup.findAll(text=lambda text: isinstance(text, Comment))
                [comment.extract() for comment in comments]

                #去掉span标签
                spans = soup.select("span")
                [span.unwrap() for span in spans]

                #去掉font标签
                fonts = soup.select("font")
                [font.unwrap() for font in fonts]

                pps = soup.select("p")
                for pp in pps:
                    del pp['style']
                    # text = pp.get_text()
                    # text = text.strip()
                    # if text is '' or len(text) < 1:#如果是空p标签,去掉
                    #     pp.extract()
                # #
                # imgs = soup.select("img")
                # for img in imgs:
                #     src = img['src']
                #     index = src.find('/')
                #     if index != -1:
                #         newSrc = 'imgs' + src[index:]
                #         img['src'] = newSrc
                #         # print newSrc
                ps = soup.select('p')
                title = ''
                for p in ps:
                    if p.get_text() != '' and len(p.get_text()) > 0:
                        title = p.get_text()
                        p.extract()
                        break
                fo = open(title + ".html", "w")
                soup.prettify()
                fo.write(str(soup));

                # 关闭打开的文件
                fo.close()

                # print soup.prettify()