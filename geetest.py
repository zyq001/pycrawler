#!/usr/local/bin/python
# -*- coding: utf8 -*-

'''
Created on 2016年9月2日

@author: PaoloLiu
'''
import urlparse

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
import PIL.Image as image
import time,re,cStringIO,urllib2,random

def get_merge_image(filename,location_list):
    '''
    根据位置对图片进行合并还原
    :filename:图片
    :location_list:图片位置
    '''
    pass

    im = image.open(filename)

    new_im = image.new('RGB', (260,116))

    im_list_upper=[]
    im_list_down=[]

    for location in location_list:

        if location['y']==-58:
            pass
            im_list_upper.append(im.crop((abs(location['x']),58,abs(location['x'])+10,166)))
        if location['y']==0:
            pass

            im_list_down.append(im.crop((abs(location['x']),0,abs(location['x'])+10,58)))

    new_im = image.new('RGB', (260,116))

    x_offset = 0
    for im in im_list_upper:
        new_im.paste(im, (x_offset,0))
        x_offset += im.size[0]

    x_offset = 0
    for im in im_list_down:
        new_im.paste(im, (x_offset,58))
        x_offset += im.size[0]

    return new_im

def get_image(driver,div):
    '''
    下载并还原图片
    :driver:webdriver
    :div:图片的div
    '''
    pass

    #找到图片所在的div
    background_images=driver.find_elements_by_xpath(div)

    location_list=[]

    imageurl=''

    for background_image in background_images:
        location={}

        #在html里面解析出小图片的url地址，还有长高的数值
        location['x']=int(re.findall("background-image: url\((.*)\); background-position: (.*)px (.*)px;",background_image.get_attribute('style'))[0][1])
        location['y']=int(re.findall("background-image: url\((.*)\); background-position: (.*)px (.*)px;",background_image.get_attribute('style'))[0][2])
        imageurl=re.findall("background-image: url\((.*)\); background-position: (.*)px (.*)px;",background_image.get_attribute('style'))[0][0]

        # location['x']=int(re.findall("background-image: url\(\"(.*)\"\); background-position: (.*)px (.*)px;",background_image.get_attribute('style'))[0][1])
        # location['y']=int(re.findall("background-image: url\(\"(.*)\"\); background-position: (.*)px (.*)px;",background_image.get_attribute('style'))[0][2])
        # imageurl=re.findall("background-image: url\(\"(.*)\"\); background-position: (.*)px (.*)px;",background_image.get_attribute('style'))[0][0]

        location_list.append(location)

    imageurl=imageurl.replace("webp","jpg")

    jpgfile=cStringIO.StringIO(urllib2.urlopen(imageurl).read())

    #重新合并图片
    image=get_merge_image(jpgfile,location_list )

    return image

def is_similar(image1,image2,x,y):
    '''
    对比RGB值
    '''
    pass

    pixel1=image1.getpixel((x,y))
    pixel2=image2.getpixel((x,y))

    for i in range(0,3):
        if abs(pixel1[i]-pixel2[i])>=50:
            return False

    return True

def get_diff_location(image1,image2):
    '''
    计算缺口的位置
    '''

    i=0

    for i in range(0,260):
        for j in range(0,116):
            if is_similar(image1,image2,i,j)==False:
                return  i

def get_track(length):
    '''
    根据缺口的位置模拟x轴移动的轨迹
    '''
    pass

    list=[]

#     间隔通过随机范围函数来获得
    x=random.randint(1,3)

    while length-x>=5:
        list.append(x)

        length=length-x
        x=random.randint(1,3)

    for i in xrange(length):
        list.append(1)

    return list

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
    from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

    caps = DesiredCapabilities.PHANTOMJS
    caps["phantomjs.page.settings.userAgent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36"
    caps["browserName"] = "Chrome"
    caps["version"] = "55.0.2883.94"
    caps["platform"] = "MacOS"
    driver = webdriver.PhantomJS(desired_capabilities=caps, executable_path=r"/Users/zyq/projects/simpleCrawler/phantomjs-2.1.1-macosx/bin/phantomjs")

    # driver = webdriver.PhantomJS(
    #     executable_path=r"/Users/zyq/projects/simpleCrawler/phantomjs-2.1.1-macosx/bin/phantomjs")
    #     driver = webdriver.Firefox()

    #     打开网页
    count = 0
    startTime = time.time()
    lastCountTime = time.time()

    baseUrl = 'http://202.108.90.73/txnS03.do'

    driver.get( baseUrl)
    # print driver.page_source
    # driver.get( baseUrl)

    for category in range(f, t + 1):

        try:
            if category > f:
                driver.refresh()

            catInputTag = driver.find_element_by_css_selector('.inputbox  input')
            catInputTag.send_keys(category)

            # nameInputTag = driver.find_element_by_css_selector('#mn')
            # nameInputTag.send_keys(word)

            submitTag = driver.find_element_by_css_selector('#_searchButton')
            submitTag.click()

            # print 'before submit url', driver.current_url
            # time.sleep(0.1)
            windowsHandler = driver.window_handles
            driver.switch_to.window(windowsHandler[1])
            # print 'after submit switch to new tab,  url', driver.current_url

            list_box = driver.find_element_by_css_selector('.list_box')
            if not list_box:
                print 'no result list, content: ',driver.page_source
                continue

            resList = list_box.find_elements_by_css_selector('tr')
            resultLength = len(resList)
            print 'result count:' , resultLength - 1
            # if resultLength == 2:
            #     print driver.page_source

            for resultIndx in range(1, resultLength):
                resTr = resList[resultIndx]
                for link in resTr.find_elements_by_css_selector('a'):
                    link.click()

                    windowsHandler = driver.window_handles
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
                        print 'count: ',count,' took ',spentTime, ' seconds, avg_per_sec: ',count / spentTime, \
                            ' last 10 spent ',(time.time() - lastCountTime),' secs'
                        lastCountTime = time.time()

                    # print 'close current detail windows'
                    driver.close()
                    print 'switch to results page'
                    driver.switch_to.window(windowsHandler[1])
                    break
            # driver.quit()
            driver.close()
            # windowsHandler = driver.window_handles
            randomTime = random.randint(500, )
            time.sleep(randomTime / 100)
            driver.switch_to.window(windowsHandler[0])
            print 'finish categoty:', category, ' reflash search page'
        except Exception as e:
            print 'id: ',category, ' error:',e

            randomTime = random.randint(100, 500)
            time.sleep(randomTime / 100)
            driver.quit()
            try:
                driver.get(baseUrl)
            except Exception as e:
                print 'reopen baseUrl fail:',e
            print 'source: ',driver.page_source

        # driver.quit()







def main():

#     这里的文件路径是webdriver的文件路径
    driver = webdriver.PhantomJS(executable_path=r"/Users/zyq/projects/simpleCrawler/phantomjs-2.1.1-macosx/bin/phantomjs")
#     driver = webdriver.Firefox()

#     打开网页
    driver.get("http://antirobot.tianyancha.com/captcha/verify?return_url=http://www.tianyancha.com/company/2320040774&rnd=ed5Ldnbo9J5wfsP3S7YUqg==")

    autoSubmitCha(driver)


    print driver.current_url
    print driver.get_cookies()

    # form = driver.find_element_by_css_selector('form[action="checkGtCaptcha"]')
    # form.submit()
    # time.sleep(5)


    print driver.current_url

    # print driver.page_source
    driver.quit()


def autoSubmitCha(driver):
    #     等待页面的上元素刷新出来
    WebDriverWait(driver, 30).until(
        lambda the_driver: the_driver.find_element_by_xpath("//div[@class='gt_slider_knob gt_show']").is_displayed())
    WebDriverWait(driver, 30).until(
        lambda the_driver: the_driver.find_element_by_xpath("//div[@class='gt_cut_bg gt_show']").is_displayed())
    WebDriverWait(driver, 30).until(
        lambda the_driver: the_driver.find_element_by_xpath("//div[@class='gt_cut_fullbg gt_show']").is_displayed())
    #     下载图片
    image1 = get_image(driver, "//div[@class='gt_cut_bg gt_show']/div")
    image2 = get_image(driver, "//div[@class='gt_cut_fullbg gt_show']/div")
    #     计算缺口位置
    loc = get_diff_location(image1, image2)
    #     生成x的移动轨迹点
    track_list = get_track(loc)
    #     找到滑动的圆球
    element = driver.find_element_by_xpath("//div[@class='gt_slider_knob gt_show']")
    location = element.location
    #     获得滑动圆球的高度
    y = location['y']
    #     鼠标点击元素并按住不放
    print "第一步,点击元素"
    ActionChains(driver).click_and_hold(on_element=element).perform()
    time.sleep(0.15)
    print "第二步，拖动元素"
    track_string = ""
    for track in track_list:
        track_string = track_string + "{%d,%d}," % (track, y - 445)
        #         xoffset=track+22:这里的移动位置的值是相对于滑动圆球左上角的相对值，而轨迹变量里的是圆球的中心点，所以要加上圆球长度的一半。
        #         yoffset=y-445:这里也是一样的。不过要注意的是不同的浏览器渲染出来的结果是不一样的，要保证最终的计算后的值是22，也就是圆球高度的一半
        ActionChains(driver).move_to_element_with_offset(to_element=element, xoffset=track + 22,
                                                         yoffset=y - 445).perform()
        #         间隔时间也通过随机函数来获得
        time.sleep(random.randint(10, 50) / 100)
    print track_string
    #     xoffset=21，本质就是向后退一格。这里退了5格是因为圆球的位置和滑动条的左边缘有5格的距离
    ActionChains(driver).move_to_element_with_offset(to_element=element, xoffset=21, yoffset=y - 445).perform()
    time.sleep(0.1)
    ActionChains(driver).move_to_element_with_offset(to_element=element, xoffset=21, yoffset=y - 445).perform()
    time.sleep(0.1)
    ActionChains(driver).move_to_element_with_offset(to_element=element, xoffset=21, yoffset=y - 445).perform()
    time.sleep(0.1)
    ActionChains(driver).move_to_element_with_offset(to_element=element, xoffset=21, yoffset=y - 445).perform()
    time.sleep(0.1)
    ActionChains(driver).move_to_element_with_offset(to_element=element, xoffset=21, yoffset=y - 445).perform()
    print "第三步，释放鼠标"
    #     释放鼠标
    ActionChains(driver).release(on_element=element).perform()
    time.sleep(2)
    #     点击验证
    #     submit=driver.find_element_by_xpath("//input[@id='submit-button']")
    submit = driver.find_element_by_id("submit-button")
    print driver.current_url
    ActionChains(driver).click(on_element=submit).perform()
    print driver.current_url
    time.sleep(5)


if __name__ == '__main__':
    pass

    # main()
    tradMarkTestById(19540000, 19550000)