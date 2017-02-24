#!/usr/bin/python
# -*- coding: UTF-8 -*-
from Config import ERAHOST


def getContent(url):

    import requests
    s = requests.Session()
    headers = {u'user-agent': u'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:49.0) Gecko/20100101 Firefox/49.0'}

    try:
        r = s.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            if r.encoding is None or r.encoding == 'ISO-8859-1':
                if r.apparent_encoding == 'GB2312':
                    r.encoding = 'gbk'
                else:
                    r.encoding = r.apparent_encoding
            return r.text
    except Exception as e:
        print e
    except IOError as er:
        print er

    return None

def getContentWithUA(url, ua):

    import requests
    s = requests.Session()
    headers = {u'user-agent': ua}

    try:
        r = s.get(url, headers=headers, timeout=60)
        if r.status_code == 200:
            if r.encoding is None or r.encoding == 'ISO-8859-1':
                if r.apparent_encoding == 'GB2312':
                    r.encoding = 'gbk'
                else:
                    r.encoding = r.apparent_encoding
            return r.text
    except Exception as e:
        print e
        try:
            r = s.get(url, headers=headers, timeout=60)
            if r.status_code == 200:
                if r.encoding is None or r.encoding == 'ISO-8859-1':
                    if r.apparent_encoding == 'GB2312':
                        r.encoding = 'gbk'
                    else:
                        r.encoding = r.apparent_encoding
                return r.text
        except Exception as e:
            print e
        except IOError as er:
            print er

    except IOError as er:
        print er
        try:
            r = s.get(url, headers=headers, timeout=60)
            if r.status_code == 200:
                if r.encoding is None or r.encoding == 'ISO-8859-1':
                    if r.apparent_encoding == 'GB2312':
                        r.encoding = 'gbk'
                    else:
                        r.encoding = r.apparent_encoding
                return r.text
        except Exception as e:
            print e
        except IOError as er:
            print er

    return None
    # r = s.get('http://httpbin.org/cookies')

def getRedictedUrl(url):
    import requests
    s = requests.Session()
    try:
        r = s.head(url)
        if r.status_code == 302:
            return r.headers['Location']
        return url
    except Exception as e:
        print '获取跳转url失败：',e

def getContentAndRedictedUrl(url):

    import requests
    s = requests.Session()

    headers = {u'user-agent': u'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:49.0) Gecko/20100101 Firefox/49.0'}

    try:
        r = s.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            # if not r.encoding.startswith('gb'):
            #     r.encoding = 'utf-8'
            if r.encoding is None or r.encoding == 'ISO-8859-1':
                if r.apparent_encoding == 'GB2312':
                    r.encoding = 'gbk'
                else:
                    r.encoding = r.apparent_encoding
            return r.text,r.url
    except Exception as e:
        print e
    except IOError as er:
        print er
    return None,None
    # r = s.get('http://httpbin.org/cookies')


# text,url = getContentAndRedictedUrl('http://b.easou.com/w/read/85356/14075857/6.html')
# print text,'  url:  ',url


def getERAConn():

    import MySQLdb
    # 打开数据库连接
    db = MySQLdb.connect(host=ERAHOST, port=3306, user="tataera",
                         passwd=ERAPASSWD, db="suixinggou_test", use_unicode=True)

    return getConnCsorBydbObj(db)


def getEADConn():

    import MySQLdb
    # 打开数据库连接
    db = MySQLdb.connect(host=EADHOST, port=3306, user="ead",
                         passwd=EADPASSWD, db="ead", use_unicode=True)

    return getConnCsorBydbObj(db)


def getConnCsorBydbObj(db):
    db.set_character_set('utf8')
    cursor = db.cursor()
    # Enforce UTF-8 for the connection.
    cursor.execute('SET NAMES utf8mb4')
    cursor.execute("SET CHARACTER SET utf8mb4")
    cursor.execute("SET character_set_connection=utf8mb4")
    return cursor, db