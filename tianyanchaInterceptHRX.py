##!/usr/bin/python
# -*- coding: UTF-8 -*-
import random
import time
import traceback

import MySQLdb
from selenium import webdriver

from Config import phantomPath, USER_AGENTS, DAVIDPASSWD
from geetest import autoSubmitCha
from proxy.ipproxy import getAvailableIPs

# mysqlPool = None
conn = None
csor = None
def getConnCsor():
    global conn,csor
    if conn and csor:
        return conn,csor

    from DBUtils.PooledDB import PooledDB

    # global mysqlPool
    # if not mysqlPool:
    mysqlPool = PooledDB(creator=MySQLdb, mincached=1, maxcached=2,
                         host="10.10.1.29", port=3306, user="root",
                         # host=NODE1IP, port=44408, user="crawler",
                 # host="10.24.161.94", port=44408, user="crawler",
                 passwd=DAVIDPASSWD, db="com", use_unicode=True, charset='utf8')
                 # passwd=NODEPASSWD, db="com_info", use_unicode=True, charset='utf8')
    conn2 = mysqlPool.connection()
    csor2 = conn2.cursor()

    # conn.set_character_set('utf8')
    csor2.execute('SET NAMES utf8')
    csor2.execute("SET CHARACTER SET utf8")
    csor2.execute("SET character_set_connection=utf8")
    return conn2,csor2


def insertOrgContent(qid, content):
    conn, csor = getConnCsor()
    csor.execute('insert ignore tyc_org_content (id, org_content) values(%s, %s)',(qid,content))
    conn.commit()

def loadWithUrl(browser,url = None):
    # document.getElementById("interceptedResponse").innerHTML +=
    # '{"data":' + self.responseText + '}*****';
    time.sleep(random.random())
    newUrl = browser.current_url
    if 'antirobot.tianyancha.com/captcha' in newUrl:
        autoSubmitCha(browser)

    if 'antirobot.tianyancha.com/captcha' in newUrl:
        return None

    if not url:
        url = browser.current_url
    qid = url.replace('http://www.tianyancha.com/company/','')
    errorPage = browser.find_elements_by_css_selector('.errorpage')
    if errorPage and len(errorPage) > 0:
        print 'content not found, qid:',qid
        return
    if '403' in browser.title or 'Forbidden' in browser.title:
        print 'forbidden, reflash ip'
        return None

    if not len(browser.page_source) < 200:

        insertOrgContent(qid, browser.page_source)
    return 'ok'



    # print  browser.execute_script('return companyDetailJson;')

def getNewBrowser(url):
    service_args, caps = getNewBrowserArgs()

    driver = webdriver.PhantomJS(executable_path=phantomPath, service_args=service_args, desired_capabilities=caps)
    # driver = webdriver.PhantomJS(executable_path=phantomPath, desired_capabilities=caps)

    driver.set_page_load_timeout(100)
    # driver.set_window_size(1366,768)

    # driver.execute_script("var companyDetailJson = ''")
    # browser.execute_script("var near = '';")
    # browser.execute_script("var near = '';")
    driver.get(url)
    # driver.execute_script("""
    # (function(XHR) {
    #   "use strict";
    #
    #   var element = document.createElement('div');
    #   element.id = "interceptedResponse";
    #   element.appendChild(document.createTextNode(""));
    #   document.body.appendChild(element);
    #
    #   var open = XHR.prototype.open;
    #   var send = XHR.prototype.send;
    #
    #   XHR.prototype.open = function(method, url, async, user, pass) {
    #     this._url = url; // want to track the url requested
    #     open.call(this, method, url, async, user, pass);
    #   };
    #
    #   XHR.prototype.send = function(data) {
    #     var self = this;
    #     var oldOnReadyStateChange;
    #     var url = this._url;
    #
    #     function onReadyStateChange() {
    #       if(self.status === 200 && self.readyState == 4 /* complete */) {
    #       if(window.companyDetailJson){
    #       window.companyDetailJson = window.companyDetailJson + self.responseText;
    #       }else{
    #
    #       }
    #           companyDetailJson = companyDetailJson + self.responseText;
    #       }
    #       if(oldOnReadyStateChange) {
    #         oldOnReadyStateChange();
    #       }
    #     }
    #
    #     if(this.addEventListener) {
    #       this.addEventListener("readystatechange", onReadyStateChange,
    #         false);
    #     } else {
    #       oldOnReadyStateChange = this.onreadystatechange;
    #       this.onreadystatechange = onReadyStateChange;
    #     }
    #     send.call(this, data);
    #   }
    # })(XMLHttpRequest);
    # """)
    # s = requests.Session()
    # cooks = driver.get_cookies()
    # cookies = dict()
    # for co in cooks:
    #     cookies[co['name']] = co['value']
    #
    # print s.get(url + '.json',cookies= cookies).text
    return driver

def getNewBrowserArgs():
    pIPs = getAvailableIPs()
    pipObj = random.choice(pIPs)
    # randomPIpIndex = random.randint(0, len(pIPs))
    # pipObj = pIPs[randomPIpIndex]
    pIp = pipObj[0]
    pPort = pipObj[1]
    # ua = random.choice(USER_AGENTS)
    # caps["phantomjs.page.settings.userAgent"] = ua
    # proxy = webdriver.Proxy()
    # proxy.proxy_type = ProxyType.MANUAL
    # proxy.http_proxy = pIp + ':' + str(pPort)
    # # 将代理设置添加到webdriver.DesiredCapabilities.PHANTOMJS中
    # proxy.add_to_capabilities(caps)
    # driver.start_session(caps)

    # if driver:
    #     try:
    #         # driver.close()
    #         driver.quit()
    #     except Exception as er:
    #         print er
    caps = webdriver.DesiredCapabilities.PHANTOMJS

    ua = random.choice(USER_AGENTS)
    caps["phantomjs.page.settings.userAgent"] = ua

    service_args = [
        '--proxy=' + pIp + ':' + str(pPort),
        '--proxy-type=http',
    ]

    return service_args,caps

def start(f=2321660000, t=2321661823):
    baseUrl = 'http://www.tianyancha.com/company/'
    step = f
    qidCarry = 1000
    newBrowserCarry = 100
    newBCount = 0
    driver = None
    while step <= t:
        seq = range(step, step + qidCarry )
        random.shuffle(seq)
        for qid in seq:
            try:
                url = baseUrl + str(qid)
                # url = baseUrl + str(2320040778)
                if newBCount == 0 or newBCount >= newBrowserCarry:
                    # if newBCount != 0:
                    newBCount = 1
                    if driver: # 先关闭上一个
                        try:
                            driver.quit()
                        except Exception as se:
                            print 'se'
                    driver = getNewBrowser(url)
                    while not loadWithUrl(driver, url = url):
                        driver = getNewBrowser(url)
                    # try:
                    #     driver.close() #只关闭标签页
                    # except Exception as ce:
                    #     print 'ce'
                else:
                    driver.get(url)
                    while not loadWithUrl(driver, url = url):
                        driver = getNewBrowser(url)
                    # try:
                    #     driver.close() #只关闭标签页
                    # except Exception as ce:
                    #     print 'ce'
                    newBCount = newBCount + 1
            except Exception as e:
                print 'fail and not retry. qid: ',str(qid), 'e:',traceback.format_exc()
                newBCount == 0


        if step == t:
            print 'Done!'
            break
        step = step + qidCarry
        step = step if step < t else t

    driver = getNewBrowser('http://www.tianyancha.com/company/3793676')
    loadWithUrl(driver)


def testChrome():
    driver = webdriver.Chrome(executable_path='/Users/zyq/env/chromedriver')
    # driver = webdriver.Chrome()
    driver.get('http://www.qichacha.com/firm_c3ece65bad28c17cc7f67168448e50e1.shtml');
    print driver.current_url
    print driver.page_source

if __name__ == '__main__':

    f = 25998711
    t = 2321661823
    import sys

    if len(sys.argv) > 2:
        f = int(sys.argv[1])
        t = int(sys.argv[2])

    # start(f,t)
    testChrome()