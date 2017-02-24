#!/usr/local/bin/python
# -*- coding: utf8 -*-

'''
Created on 2016年9月2日

@author: zyq
'''
import traceback
import urlparse
# from telnetlib import EC
from selenium.webdriver.support import expected_conditions as EC

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.proxy import ProxyType
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
import PIL.Image as image
import time,re,cStringIO,urllib2
import random

from Config import minPIPCount, USER_AGENTS, phantomPath, phantomBrowserNames
from proxy.ipproxy import getAvailableIPs, deletByIP

keyStamp = [1486693909951,1486693910245,1486693910638,1486693912697,1486693913091,1486693914631,1486693915013,1486693915313,1486693916550,1486693917430,1486693917799,1486693919750]
keyS2 = []
for ind in range(0,len(keyStamp)):
    if ind == 0:
        keyS2.append(0)
    else:
        keyS2.append(keyStamp[ind] - keyStamp[ind - 1])

def tradMarkTest(word):
    driver = webdriver.PhantomJS(
        executable_path=r"/Users/zyq/projects/simpleCrawler/phantomjs-2.1.1-macosx/bin/phantomjs")
    #     driver = webdriver.Firefox()

    #     打开网页

    baseUrl = 'http://202.108.90.73/txnS01.do'

    driver.get( baseUrl)

    for category in range(1, 46):

        if category > 1:
            driver.refresh()

        catInputTag = driver.find_element_by_css_selector('#nc')
        catInputTag.send_keys(category)

        nameInputTag = driver.find_element_by_css_selector('#mn')
        nameInputTag.send_keys(word)

        submitTag = driver.find_element_by_css_selector('#_searchButton')
        submitTag.click()

        print 'before submit url', driver.current_url
        # time.sleep(0.1)
        windowsHandler = driver.window_handles
        driver.switch_to.window(windowsHandler[1])
        print 'after submit switch to new tab,  url', driver.current_url

        list_box = driver.find_element_by_css_selector('.list_box')
        if not list_box:
            print 'no result list, content: ',driver.page_source
            continue

        resList = list_box.find_elements_by_css_selector('tr')
        resultLength = len(resList)
        print 'result count:' , resultLength - 1
        if resultLength == 2:
            print driver.page_source

        for resultIndx in range(1, resultLength):
            resTr = resList[resultIndx]
            for link in resTr.find_elements_by_css_selector('a'):
                link.click()

                windowsHandler = driver.window_handles
                driver.switch_to.window(windowsHandler[2])

                # linkUr = urlparse.urljoin(baseUrl, link['href'])
                print 'walking result urls , now :', driver.current_url
                # driver.get(linkUr)

                print 'deatail page length: ', len(driver.page_source)

                datas = []
                for info in driver.find_elements_by_css_selector('.info'):
                    if(len(info.text) < 20):
                        datas.append(info.text)

                print 'datas:',repr(datas).decode("unicode-escape")
                print 'close current detail windows'
                driver.close()
                print 'switch to results page'
                driver.switch_to.window(windowsHandler[1])
                break
        # driver.quit()
        driver.close()
        # windowsHandler = driver.window_handles
        driver.switch_to.window(windowsHandler[0])
        print 'finish categoty:', category, ' reflash search page'

        # driver.quit()


def tradMarkTestById( f, t):
    # from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

    # driver = webdriver.PhantomJS(executable_path=phantomPath,service_args=service_args)
    # caps = webdriver.DesiredCapabilities.PHANTOMJS

    ua = random.choice(USER_AGENTS)
    # caps["phantomjs.page.settings.userAgent"] = ua

    pIPs = getAvailableIPs()
    print 'start with ' + str(len(pIPs)) + ' proxy ips'


    # startWithDriver(driver, f, t)
    baseUrl = 'http://202.108.90.73/txnS03.do'
    count = 0
    startTime = time.time()
    lastCountTime = time.time()

    # pIPs, pIp, randomPIpIndex = reflashProxy(caps, driver, pIPs)

    pipOk = False

    maxCountPerProxy = 5
    nowCount = 0

    driver = None

    noNeedReStart = False

    for category in range(f, t + 1):

        if not pipOk or nowCount > maxCountPerProxy:
            # pIPs, pIp, randomPIpIndex = reflashProxy(caps, driver, pIPs)
            if len(pIPs) < minPIPCount:
                # 代理ip太少，重新获取
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

            if driver:
                try:
                    # driver.close()
                    driver.quit()
                except Exception as er:
                    print er
            caps = webdriver.DesiredCapabilities.PHANTOMJS

            ua = random.choice(USER_AGENTS)
            caps["phantomjs.page.settings.userAgent"] = ua

            service_args = [
                '--proxy=' + pIp + ':' + str(pPort),
                '--proxy-type=http',
            ]

            noNeedReStart = False

            nowCount = 0
        else:
            nowCount = nowCount + 1

        try:

            if not noNeedReStart:
                driver = webdriver.PhantomJS(executable_path=phantomPath, service_args=service_args, desired_capabilities=caps)

                driver.set_page_load_timeout(30)
                # driver.set_window_size(1366,768)
                driver.get(baseUrl)
            else:
                driver.refresh()

            pipOk = True

            catInputTag = driver.find_element_by_css_selector('.inputbox input')
            catInputTag.send_keys(category)

            # nameInputTag = driver.find_element_by_css_selector('#mn')
            # nameInputTag.send_keys(word)

            submitTag = driver.find_element_by_css_selector('#_searchButton')
            submitTag.click()

            # print 'before submit url', driver.current_url
            time.sleep(0.1)
            windowsHandler = driver.window_handles
            if len(windowsHandler) < 2:
                print 'not open result page, skip'
                noNeedReStart = True
                continue
            driver.switch_to.window(windowsHandler[1])
            # print 'after submit switch to new tab,  url', driver.current_url
            list_box = driver.find_element_by_css_selector('.list_box')
            if not list_box:
                print 'no result list, content: ', driver.page_source
                noNeedReStart = True
                continue

            resList = list_box.find_elements_by_css_selector('tr')
            resultLength = len(resList)
            print 'result count:', resultLength - 1
            # if resultLength == 2:
            #     print driver.page_source

            for resultIndx in range(1, resultLength):
                resTr = resList[resultIndx]
                for link in resTr.find_elements_by_css_selector('a'):
                    link.click()

                    windowsHandler = driver.window_handles

                    if len(windowsHandler) < 2:
                        print 'not open result page, skip'
                        noNeedReStart = True

                        continue
                    driver.switch_to.window(windowsHandler[2])

                    # linkUr = urlparse.urljoin(baseUrl, link['href'])
                    print 'walking result urls , now :', driver.current_url
                    # driver.get(linkUr)

                    # print 'deatail page length: ', len(driver.page_source)

                    # datas = []
                    # for info in driver.find_elements_by_css_selector('.info'):
                    #     if(len(info.text) < 20):
                    #         datas.append(info.text)

                    # print 'datas:',repr(datas).decode("unicode-escape")
                    count = count + 1
                    if count % 10 == 0:
                        spentTime = time.time() - startTime
                        print 'count: ', count, ' took ', spentTime, ' seconds, avg_per_sec: ', count / spentTime, \
                            ' last 10 spent ', (time.time() - lastCountTime), ' secs'
                        lastCountTime = time.time()

                    # print 'close current detail windows'
                    # driver.close()


                    try:
                        driver.close()
                        # driver.quit()
                    except Exception as er:
                        print er

                    print 'switch to results page'
                    driver.switch_to.window(windowsHandler[1])

                    try:
                        driver.close()
                        # driver.quit()
                    except Exception as er:
                        print er
                    break
            # driver.quit()
            # driver.close()
            # windowsHandler = driver.window_handles
            # randomTime = random.randint(500, )
            # time.sleep(randomTime / 100)

            driver.switch_to.window(windowsHandler[0])
            # driver.close()
            # driver.quit()

            # try:
            #     driver.close()
            #     # driver.quit()
            # except Exception as er:
            #     print er
            # try:
            #     # driver.close()
            #     driver.quit()
            # except Exception as er:
            #     print er
            noNeedReStart = True

            print 'finish categoty:', category, ' reflash search page'
        except Exception as e:
            print 'id: ', category, ' error:', e
            print 'proxy ip:',pIp,' port:',pPort

            # randomTime = random.randint(100, 500)
            # time.sleep(randomTime / 100)
            # print 'source: ', driver.page_source

            # driver.quit()
            try:
                driver.close()
                # driver.quit()
            except Exception as er:
                print er
            try:
                # driver.close()
                driver.quit()
            except Exception as er:
                print er
            noNeedReStart = False

            # del pIPs[randomPIpIndex]
            pIPs.remove(pipObj)
            # deletByIP(pIp)

            pipOk = False
            # try:
            #     driver.get(baseUrl)
            # except Exception as e:
            #     print 'reopen baseUrl fail:', e


def reflashProxy(caps, driver, pIPs):
    if len(pIPs) < minPIPCount:
        # 代理ip太少，重新获取
        pIPs = getAvailableIPs()
    # pipObj = random.choice(pIPs)
    randomPIpIndex = random.randint(0, len(pIPs))
    pipObj = pIPs[randomPIpIndex]
    pIp = pipObj[0]
    pPort = pipObj[1]
    ua = random.choice(USER_AGENTS)
    caps["phantomjs.page.settings.userAgent"] = ua
    proxy = webdriver.Proxy()
    proxy.proxy_type = ProxyType.MANUAL
    proxy.http_proxy = pIp + ':' + str(pPort)
    # 将代理设置添加到webdriver.DesiredCapabilities.PHANTOMJS中
    proxy.add_to_capabilities(caps)
    driver.start_session(caps)
    return pIPs, pIp, randomPIpIndex


def shangbiaoOneIP(f, t):
    caps = webdriver.DesiredCapabilities.PHANTOMJS

    ua = random.choice(USER_AGENTS)
    caps["phantomjs.page.settings.userAgent"] = ua

    driver = webdriver.PhantomJS(executable_path=phantomPath, desired_capabilities=caps)

    driver.set_page_load_timeout(30)
    # driver.set_window_size(1366,768)
    # driver.get(baseUrl)
    # oldStartWithDriver(driver, f, t)


def getPaths():
    filePaths = open('mousePaths')
    paths = []
    startStamp = -1
    while 1:
        line = filePaths.readline()
        if not line:
            break
        point = []
        line = line.replace('\n', '')
        pos = line.split(',')
        point.append( pos[0])
        point.append( pos[1])
        # point[1] = pos[1]
        stamp = pos[2]
        if startStamp < 0:
            startStamp = stamp
        # point[2] = stamp - startStamp
        point.append(long(stamp) - long(startStamp))
        startStamp = stamp

        paths.append(point)

    return paths

def getPaths2():
    filePaths = open('mousePaths2')
    paths = []
    startStamp = -1
    while 1:
        line = filePaths.readline()
        if not line:
            break
        point = []
        line = line.replace('\n', '')
        pos = line.split(',')
        point.append(pos[0])
        point.append(pos[1])
        # point[1] = pos[1]
        stamp = pos[2]
        if startStamp < 0:
            startStamp = stamp
        # point[2] = stamp - startStamp
        point.append(long(stamp) - long(startStamp))
        startStamp = stamp

        paths.append(point)

    return paths

def oldStartWithDriver( f, t):
    # caps = webdriver.DesiredCapabilities.PHANTOMJS

    # ua = random.choice(USER_AGENTS)
    # ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36'
    #
    # headers = {
    #             'Accept': '*/*',
    #            # 'Accept-Encoding': 'gzip, deflate',
    #            'Accept-Language': 'en-US,en,zh_CN;q=0.8',
    #            'Connection': 'keep-alive',
    #            'Cache-Control': 'max-age=0'
    #            }
    #
    # for key, value in headers.iteritems():
    #     caps['phantomjs.page.customHeaders.{}'.format(key)] = value

    # caps["phantomjs.page.settings.userAgent"] = ua
    # ranBrow = random.choice(phantomBrowserNames)
    # caps["browserName"] = ranBrow
    # caps['platform'] = ua
    # caps['version'] =


    # driver = webdriver.PhantomJS(executable_path=phantomPath, desired_capabilities=caps)
    driver = webdriver.Chrome(executable_path='/Users/zyq/env/chromedriver')

    # driver.set_page_load_timeout(30)

    baseUrl = 'http://202.108.90.73/txnS03.do'
    # baseUrl = 'file:///Users/zyq/projects/crawl/trademark/%E5%95%86%E6%A0%87%E7%8A%B6%E6%80%81%E6%A3%80%E7%B4%A22.html'
    count = 0
    startTime = time.time()
    lastCountTime = time.time()
    driver.get(baseUrl)

    paths = getPaths()
    paths2 = getPaths2()

    # driver.
    # time.sleep(600)

    for category in range(f, t + 1):

        try:
            if category > f:
                driver.refresh()

            y = 600
            # for track in [random.randint(800, 1366)]:
            #     track_string = track_string + "{%d,%d}," % (track, y - 445)
            #     #         xoffset=track+22:这里的移动位置的值是相对于滑动圆球左上角的相对值，而轨迹变量里的是圆球的中心点，所以要加上圆球长度的一半。
            #     #         yoffset=y-445:这里也是一样的。不过要注意的是不同的浏览器渲染出来的结果是不一样的，要保证最终的计算后的值是22，也就是圆球高度的一半
            #     ActionChains(driver).move_to_element_with_offset( xoffset=track + 22,
            #                                                      yoffset=y - 445).perform()
            #     #         间隔时间也通过随机函数来获得
            #     time.sleep(random.randint(10, 50) / 100)


            # time.sleep(200)


            # catInputTag = driver.find_element_by_css_selector('.inputbox input')
            catInputTag = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".inputbox input")))

            inputPos = catInputTag.location
            print 'dest1:',inputPos
            inputX = inputPos['x']
            inputY = inputPos['y']
            inputX = inputPos['x']
            inputY = inputPos['y']

            for point in paths:
                # xoff = int(point[0]) - 100
                # yoff = int(point[1]) - 17
                xoff = int(point[0])
                yoff = int(point[1])





                sleepTime = point[2]
                print 'now pos: ',str(xoff),str(yoff),'offset: ',str(inputX - xoff),str(inputY - yoff)

                ActionChains(driver).move_to_element_with_offset(catInputTag,inputX - xoff, inputY - yoff).perform()

                time.sleep(sleepTime / float(1000))

            # ActionChains(driver).move_to_element(catInputTag).perform()
            time.sleep(random.random())

            # act = ActionChains(driver).click(catInputTag).perform()


            ActionChains(driver).click_and_hold(catInputTag).perform()
            time.sleep(0.9)
            ActionChains(driver).release(catInputTag).perform()


            time.sleep(random.randint(1, 10) / float(1000))

            # time.sleep(60)


            # for c in str(category):
            for i in range(0, len(str(category))):
                c = str(category)[i]
                # act.send_keys(c)
                catInputTag.send_keys(c)

                time.sleep(keyS2[i + 1] / float(1000))

            # catInputTag.send_keys(category)

            # nameInputTag = driver.find_element_by_css_selector('#mn')
            # nameInputTag.send_keys(word)

            # time.sleep(random.randint(0.1,2))
            # time.sleep(random.random() * 2)
            # time.sleep(0.5)

            # time.sleep(600)


            # submitTag = driver.find_element_by_css_selector('#_searchButton')
            submitTag = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#_searchButton")))

            inputPos = submitTag.location
            print 'dest1:',inputPos

            inputX2 = inputPos['x']
            inputY2 = inputPos['y']

            for point in paths2:
                xoff = int(point[0])
                yoff = int(point[1])
                sleepTime = point[2]
                print 'now pos: ',str(xoff),str(yoff),'offset: ',str(inputX2 - xoff),str(inputY2 - yoff)


                ActionChains(driver).move_to_element_with_offset(submitTag, inputX2 - xoff, inputY2 - yoff).perform()

                time.sleep(sleepTime / float(1000))

            # ActionChains(driver).move_to_element(submitTag).perform()

            # ActionChains(driver).move_to_element(catInputTag)
            time.sleep(random.randint(1, 10) / float(1000))

            # time.sleep(600)


            # submitTag.click()
            ActionChains(driver).click_and_hold(submitTag).perform()
            time.sleep(0.8)
            ActionChains(driver).release(submitTag).perform()

            # ActionChains(driver).click().perform()
            # print 'before submit url', driver.current_url
            # time.sleep(random.random() )
            time.sleep(random.randint(1, 10) / float(1000))

            time.sleep(600)


            windowsHandler = driver.window_handles
            driver.switch_to.window(windowsHandler[1])
            # print 'after submit switch to new tab,  url', driver.current_url


            list_box = driver.find_element_by_css_selector('.list_box')
            if not list_box:
                print 'no result list, content: ', driver.page_source
                continue

            resList = list_box.find_elements_by_css_selector('tr')
            resultLength = len(resList)
            print 'result count:', resultLength - 1
            # if resultLength == 2:
            #     print driver.page_source
            # time.sleep(random.random())
            # time.sleep(random.random())

            for resultIndx in range(1, resultLength):
                resTr = resList[resultIndx]
                for link in resTr.find_elements_by_css_selector('a'):
                    link.click()

                    # time.sleep(random.randint(0.1, 1))
                    time.sleep(random.random())
                    # time.sleep(1)

                    windowsHandler = driver.window_handles
                    driver.switch_to.window(windowsHandler[2])

                    # linkUr = urlparse.urljoin(baseUrl, link['href'])
                    print u'爬取成功， 最终链接为:', driver.current_url
                    # driver.get(linkUr)

                    # print 'deatail page length: ', len(driver.page_source)

                    # datas = []
                    # for info in driver.find_elements_by_css_selector('.info'):
                    #     if(len(info.text) < 20):
                    #         datas.append(info.text)

                    # print 'datas:',repr(datas).decode("unicode-escape")
                    count = count + 1
                    if count % 10 == 0:
                        spentTime = time.time() - startTime
                        print u'共成功: ', count, u' 次, 花费 ', spentTime, u' 秒, 平均一秒: ', count / spentTime, \
                            u'个, 最后10个花费： ', (time.time() - lastCountTime), u' 秒'
                        lastCountTime = time.time()

                    # print 'close current detail windows'
                    driver.close()
                    print 'switch to results page'
                    driver.switch_to.window(windowsHandler[1])
                    break
            # driver.quit()
            driver.close()
            # windowsHandler = driver.window_handles
            # randomTime = random.randint(50, 200)
            # time.sleep(randomTime / 100)
            # time.sleep(random.random())
            driver.switch_to.window(windowsHandler[0])
            print 'finish categoty:', category, ' reflash search page'
        except Exception as e:
            print 'id: ', category, u' 爬取失败:', traceback.format_exc()
            # print 'update size exception, ', traceback.format_exc()
            # if driver:
            #     print driver.page_source

            # randomTime = random.randint(100, 500)
            # time.sleep(randomTime / 100)
            driver.quit()

            driver = webdriver.PhantomJS(executable_path=phantomPath, desired_capabilities=caps)

            driver.set_page_load_timeout(30)

            try:
                driver.get(baseUrl)
            except Exception as e:
                print 'reopen baseUrl fail:', e
            # print 'source: ', driver.page_source

            # driver.quit()


if __name__ == '__main__':
    pass

    # paths = getPaths()
    # main()
    # tradMarkTestById(19540000, 19550000)
    oldStartWithDriver(19530000, 19540000)