##!/usr/bin/python
# -*- coding: UTF-8 -*-
import json
import random
import urlparse

import MySQLdb
import requests
from selenium.webdriver.phantomjs import webdriver

from Config import phantomPath, NODE1IP, DAVIDPASSWD
from framework.config import getBloom
from framework.htmlParser import getSoupByStrEncode

ua = 'Mozilla/5.0 (Linux; U; Android 4.0; en-us; Xoom Build/HRI39) AppleWebKit/534.13 (KHTML, like Gecko) Version/4.0 Safari/534.13'
from networkHelper import getContentWithUA

idBloom = []

def getQichachaHtml(url):
    cookies = {'PHPSESSID':'bhtvcmlegu7r8helh07sm85j87'}

    s = requests.Session()
    r = s.get(url, timeout=30, cookies=cookies)
    if r.status_code == 200:
        if r.encoding is None or r.encoding == 'ISO-8859-1':
            if r.apparent_encoding == 'GB2312':
                r.encoding = 'gbk'
            else:
                r.encoding = r.apparent_encoding
        return r.text
    print None


def getBaseInfoById(prov = None, uid = ''):
    if prov:
        url = 'http://app.qichacha.com/enterprises/v6/new/newGetData?province=' + prov + '&unique=' + uid
    else:
        url = 'http://app.qichacha.com/enterprises/v6/new/newGetData?unique=' + uid
    # cookies = {'PHPSESSID':'ad9c70m8meinmmm67kg0a8kn12'}

    s = requests.Session()
    # r = s.get(url, timeout=30, cookies=cookies)
    r = s.get(url, timeout=30)
    if r.status_code == 200:
        if r.encoding is None or r.encoding == 'ISO-8859-1':
            if r.apparent_encoding == 'GB2312':
                r.encoding = 'gbk'
            else:
                r.encoding = r.apparent_encoding
        return r.text
    print None



def qichachaFromProvs(provs):
    print 'start: provs', str(provs)
    catBaseIrl = 'http://www.qichacha.com/gongsi_area_prov_'
    conn, csor = getConnCsor()
    for prov in provs:
        pageBaseUrl = catBaseIrl + prov + '_p_'
        for pageCount in range(1, 501):
            pageUrl = pageBaseUrl + str(pageCount) + '.shtml'
            pageContent = getQichachaHtml(pageUrl)
            pageSoup = getSoupByStrEncode(pageContent, 'utf-8')
            dealUIDsBySoup(conn, csor, pageCount, pageSoup, prov)

def qichachaFromIndustry(f,t):
    print 'start from ', f, ' to ', t
    indBaseUrl = 'http://www.qichacha.com/gongsi_industry?industryCode='
    conn, csor = getConnCsor()
    for code in range(f, t+1):
        industCode = chr(code + 65)
        industOrder = code
        inductBasePageUrl = indBaseUrl + industCode + '&industryorder=' + str(industOrder)

        try:
            print 'start indust base pages, ',inductBasePageUrl
            # qichachaFromIndustPageUrl(inductBasePageUrl,conn, csor)
            print 'end indust base pages, ',inductBasePageUrl

            print 'start indust subIndust pages, ',inductBasePageUrl
            pageContent = getQichachaHtml(inductBasePageUrl)
            pageSoup = getSoupByStrEncode(pageContent, 'utf-8')
            subUrlTags = pageSoup.select('.filter-tag')[1]
            if not subUrlTags:
                print 'no subUrls, skipped,',inductBasePageUrl
            for tag in subUrlTags.select('a'):
                subUri = tag['href']
                subUrl = urlparse.urljoin(indBaseUrl,subUri)

                print 'start sub indust base pages, ', subUrl
                qichachaFromIndustPageUrl(subUrl,conn, csor)
                print 'end sub indust base pages, ', subUrl
        except Exception as e:
            print 'indust error, industCode:',industCode, ' url:',inductBasePageUrl, ' error:',e


def qichachaFromIndustPageUrl(url,conn, csor):
    baseUrl = url.replace('?','_').replace('&','_').replace('=','_') + '_p_'


    for pageCount in range(1,501):
        pageUrl = baseUrl + str(pageCount) + '.shtml'

        try:
            pageContent = getQichachaHtml(pageUrl)
            pageSoup = getSoupByStrEncode(pageContent, 'utf-8')
            dealUIDsBySoup(conn, csor, pageCount, pageSoup, 'indust')
        except Exception as e:
            print 'page error, url:',pageUrl

def dealUIDsBySoup(conn, csor, pageCount, pageSoup, prov):
    uidList = pageSoup.select('.list-group-item')
    if len(uidList) < 1:
        print 'no com list, skip ', prov, ' page:', pageCount
        return
        # continue
    for uidTag in uidList:
        try:
            if not uidTag.has_attr('href'):
                print 'no com Tag, skip ', prov, ' page:', pageCount, ' tag:', uidTag
                # continue
                return
            prv = None
            uid = uidTag['href'].replace('firm_', '').replace('.shtml', '').replace('/', '')
            if '_' in uid:
                strs = uid.split('_')
                prv = strs[0]
                uid = strs[1]
            if  uid in idBloom:
                print 'already crawled, skip uid:',uid
                continue
            com_base_info_str = getBaseInfoById(prv, uid)

            com_base_info_json = json.loads(com_base_info_str)
            if com_base_info_json['status'] != 1:
                print 'json int not succ , uid: ', uid, ' content:', com_base_info_str
            data = com_base_info_json['data']['Company']

            companyType = data['EconKind']
            # webName = data['webName']
            companyName = data['Name']
            liscense = data['No']
            if not liscense:
                liscense = data['OrgNo']
            examineDate = ''
            if  data['CheckDate']:
                examineDate = data['CheckDate'].strip()
                # webSite = ','.join(data['webSite'])
                # sql = """insert ignore into com_base (id,companyName,companyType,examineDate,liscense,source,webSite,webName) values (%s,%s,%s,%s,%s,%s,%s,%s);""" % (str(id), companyName, companyType,examineDate, liscense, "tianyacha",webSite,webName)
            try:
                csor.execute(
                    """insert ignore into com_base_copy (id,companyName,companyType,examineDate,liscense,source,src_content) values (%s,%s,%s,%s,%s,%s,%s);""",
                    (uid, companyName, companyType, examineDate, liscense, "qichacha", com_base_info_str))
                conn.commit()
                print 'comOk, uid:', uid, ', comName: ', unicode(companyName).encode('utf-8')
            except Exception as e:
                print 'insert error, uid', uid, ' ,error', e
                #     # 发生错误时回滚
        except Exception as ee:
            print 'uid:', uid, ' , error:', ee
            # com_name = com_base_info_json['data']['Company']['Name']
            # com_name = com_base_info_json['data']['Company']['Name']


def test():
    url = 'http://www.xxsy.net/books/845281/7458748.html'

    s = requests.Session()
    headers = {u'user-agent':
                   u'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36'
               ,u'Referer':u'http://www.tianyancha.com/company/2320040774'}

    cookies = {

    #     'aliyungf_tc': 'AQAAANFWQncwHAkARiduJN+90vmPq0Eo',
    #     'TYCID':'7a9911a0e990419eaae230e244bf00f4',
    # 'tnet':'36.110.39.70',
    #     '_pk_ref.1.e431':'%5B%22%22%2C%22%22%2C1478772233%2C%22https%3A%2F%2Fwww.baidu.com%2Flink%3Furl%3DrYKgdoUFBSOzS1ePjuKVrZfbeNFDt4cUThbZGmWHRFOanBaiacxZCiqainV7Jm0H%26wd%3D%26eqid%3Df4dc80a40000ad6100000004582445fe%22%5D',
    #               'Hm_lvt_e92c8d65d92d534b0fc290df538b4758':'1478772233,1478772321',
    #                                                                                                                                                                                                                                                                                         'Hm_lpvt_e92c8d65d92d534b0fc290df538b4758':'1478774127',
    #     '_pk_id.1.e431':'2f67c9a76b72d429.1478772233.1.1478772535.1478772233.',
    #     '_pk_ses.1.e431':'*',
    #               'token':'f6455c3872144bd49a343acbc2f2604b',
    #     '_utm':'9637404dcb4047d9b46b437012d7a0fa'

        'gr_user_id':'58537bdf-2eb9-4157-8fc9-e3beee9230f4',
        '_uab_collina':'147877694825876541041818','_umdata':'A502B1276E6D5FEFBBE0162D7645D3B55C35FA9F8827FF2DD0065CE55222E484B81992CDB88505A993D1A4708444AD729AD47EF962A63B4E3732C3C4DC1848FC10967B66CE1C969F61D3A8FD08667CEDACD717845CEBA8FC08CFC640A0B49967','PHPSESSID':'b0motfssliqe81vo7blp4pt904','gr_session_id_9c1eb7420511f8b2':'77119cb6-3a5b-4e22-91ae-bf316535a95f', 'CNZZDATA1254842228':'1502541019-1478775981-null%7C1478775981'

    }

    r = s.get(url, headers=headers, timeout=30, cookies=cookies)
    if r.status_code == 200:
        if r.encoding is None or r.encoding == 'ISO-8859-1':
            if r.apparent_encoding == 'GB2312':
                r.encoding = 'gbk'
            else:
                r.encoding = r.apparent_encoding
        print r.text
    print None

def getConnCsor():

    from DBUtils.PooledDB import PooledDB

    pool2 = PooledDB(creator=MySQLdb, mincached=1, maxcached=3,
                     # host="10.10.1.29", port=3306, user="root",
                     host=NODE1IP, port=44408, user="crawler",
                     # host="10.24.161.94", port=44408, user="crawler",
                     passwd=DAVIDPASSWD, db="com", use_unicode=True, charset='utf8')
                     # passwd=NODEPASSWD, db="com_info", use_unicode=True, charset='utf8')
    conn2 = pool2.connection()
    csor2 = conn2.cursor()

    # conn.set_character_set('utf8')
    csor2.execute('SET NAMES utf8')
    csor2.execute("SET CHARACTER SET utf8")
    csor2.execute("SET character_set_connection=utf8")
    return conn2,csor2

def crawlBaseInfo(begin, end):
    print 'start from ',begin,' to ',end
    baseUrl = 'http://www.tianyancha.com/IcpList/'
    conn, csor = getConnCsor()
    seq = range(begin, end)

    random.shuffle(seq)
    for id in seq:
        try:
            dealById(baseUrl, conn, csor, id)
        except Exception as e:
            print id,':  ',e


def dealById(baseUrl, conn, csor, id):
    # slp = random.randint(1, 100)
    # time.sleep(0.01 * slp)
    url = baseUrl + str(id) + '.json'
    content = getContentWithUA(url, ua)
    if not content or len(content) < 60:
        print id, 'content', content
        # continue
        return
    jsonObj = json.loads(content)
    data = jsonObj['data'][0]
    if not data or len(str(data)) < 10:
        print id, 'data:', data
        return
        # continue
    companyType = data['companyType']
    webName = data['webName']
    companyName = data['companyName']
    liscense = data['liscense']
    examineDate = data['examineDate'].strip()
    webSite = ','.join(data['webSite'])
    # sql = """insert ignore into com_base (id,companyName,companyType,examineDate,liscense,source,webSite,webName) values (%s,%s,%s,%s,%s,%s,%s,%s);""" % (str(id), companyName, companyType,examineDate, liscense, "tianyacha",webSite,webName)
    try:
        csor.execute(
            """insert ignore into com_base (id,companyName,companyType,examineDate,liscense,source,webSite,webName) values (%s,%s,%s,%s,%s,%s,%s,%s);""",
            (str(id), companyName, companyType, examineDate, liscense, "tianyacha", webSite, webName))
        conn.commit()
    except Exception as e:
        #     # 发生错误时回滚
        print e

def getWebDriver():
    caps = webdriver.DesiredCapabilities.PHANTOMJS

    # ua = random.choice(USER_AGENTS)
    ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36'

    headers = {
        'Accept': '*/*',
        # 'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en,zh_CN;q=0.8',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0'
    }

    for key, value in headers.iteritems():
        caps['phantomjs.page.customHeaders.{}'.format(key)] = value

    # caps["phantomjs.page.settings.userAgent"] = ua
    # ranBrow = random.choice(phantomBrowserNames)
    # caps["browserName"] = ranBrow
    # caps['platform'] = ua
    # caps['version'] =


    driver = webdriver.PhantomJS(executable_path=phantomPath, desired_capabilities=caps)
    # driver = webdriver.Chrome(executable_path='/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome')

    driver.set_page_load_timeout(30)

def tycFromPage():

    idbloom = getBloom()

    homeUrl = 'http://www.tianyancha.com/'
    driver = getWebDriver()
    driver.get(homeUrl)

    # for citylinkTag in driver.find_elements_by_css_selector('div[ng-show="proTitle;"] a'):
    #
    #
    # for indlinkTag in driver.find_elements_by_css_selector('div[ng-show="proTitle;"] a'):

def getQichachaDigests():
    idbloom = getBloom()
    conn, csor = getConnCsor()
    csor.execute('select id from com_base_copy')
    ids = csor.fetchall()
    [idbloom.add(mid[0]) for mid in ids]
    if ids[0][0] in idbloom:
        print 'load exists ids ok'

    return idbloom


if __name__ == '__main__':

    provs = ["AH", "BJ", "CQ", "FJ", "GS", "GD", "GX", "GZ", "HAIN",
             "HB", "HLJ", "HEN", "HUB", "HUN", "JS", "JX", "JL", "LN",
             "NMG", "NX", "QH", "SD", "SH", "SX", "SAX", "SC", "TJ",
             "XJ", "XZ", "YN", "ZJ", "CN"]
    #
    #
    # import sys
    # if len(sys.argv)> 1:
    #     inputProvs = sys.argv[1]
    #     provs = []
    #     for p in inputProvs.split(','):
    #         provs.append(p)
    #         print 'doneProv,',p
    idBloom = getQichachaDigests()
    qichachaFromProvs(provs)

    f = 0
    t = 19
    import sys
    if len(sys.argv) > 2:
        f = int(sys.argv[1])
        t = int(sys.argv[2])
    qichachaFromIndustry(f,t)

            # begin = 7100000
    # end = 7200000
    #
    # import sys
    # if len(sys.argv) > 2:
    #     begin = sys.argv[1]
    #     end = sys.argv[2]
    # crawlBaseInfo(int(begin), int(end))

    # crawlBaseInfo(2321660000, 2321661823)
