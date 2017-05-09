##!/usr/bin/python
# -*- coding: UTF-8 -*-
import smtplib
import sys
import time
from email.header import Header
from email.mime.text import MIMEText

import requests
import sqlite3
from framework.htmlParser import getSoupByStr
from gConfig import loadYaml
from Config import MAILPASS, NO_REPLY_TATATIMES_COM, SMTP_EXMAIL_QQ_COM, RECIEVERS

conn = sqlite3.connect('sites.db')
csor = conn.cursor()
# conn.execute('''CREATE TABLE SITES
#        (ID INTEGER PRIMARY KEY  AUTOINCREMENT   NOT NULL,
#        content           TEXT    NOT NULL,
#        url        CHAR(50)  UNIQUE,
#        modTime        CHAR(20)
#   );''')

# print "Table created successfully";

def getQichachaHtml(url, proxy=None):
    cookies = {'PHPSESSID':'78llsj0nbd58n7acsf98jqjre2'}

    # if not proxy:
    #     proxy = getProxy()

    s = requests.Session()
    try:
        r = s.get(url, timeout=30, cookies=cookies,proxies=proxy)
    except Exception as e:
        print u'抓取网页失败：', url
        # proxy = getProxy(True)

        try:
            r = s.get(url, timeout=30, cookies=cookies, proxies=proxy)
        except Exception as e:
            print u'抓取网页失败：', url
            return None

    if r.status_code >= 200 and r.status_code < 300 :
        if r.encoding is None or r.encoding == 'ISO-8859-1':
            if r.apparent_encoding == 'GB2312':
                r.encoding = 'gbk'
            else:
                r.encoding = r.apparent_encoding
        return r.text
    else:
        print u'return code: ',r.status_code
    print None

def getFomContentByUrl(url):
    try:
        csor.execute("SELECT content from SITES where url=?" , (url,))
    except Exception as e:
        print 'get content from db error, e: ',e
        csor.execute("SELECT content from SITES where url=?" , (url, ))
    conn.commit()
    res = csor.fetchone()
    if  res:
        content = res[0]
        return content

def UpdateContentByUrl(url, newContent):
    try:
        csor.execute("update SITES set content =?  where url=?" , (newContent, url))
        conn.commit()
    except Exception as e:
        print 'update content from db error, e: ',e
        csor.execute("update SITES set content =?  where url=?" , (newContent, url))
        conn.commit()

def InsertContentByUrl(url, newContent):
    try:
        csor.execute("INSERT OR IGNORE INTO SITES ( content, url) VALUES  (?, ?) " , (newContent, url))
        conn.commit()
    except Exception as e:
        print 'update content from db error, e: ',e
        csor.execute("INSERT OR IGNORE INTO  SITES ( content, url) VALUES  (?, ?) " , (newContent, url))
        conn.commit()

def sendEmail(newContent, oldContent, url):
    mail_host = SMTP_EXMAIL_QQ_COM  # 设置服务器
    mail_user = NO_REPLY_TATATIMES_COM  # 用户名
    mail_pass = MAILPASS  # 口令

    sender = NO_REPLY_TATATIMES_COM
    receivers = [RECIEVERS]  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

    msg = u'''
    <table class="table table-striped m-b-none text-sm" style="background-color: transparent;border-collapse: collapse;border-spacing: 0;box-sizing: border-box;display: table;border-collapse: separate;border-spacing: 2px;border-color: grey;">
    <thead>
        <tr role="row">
            <th>新内容</th>
            <th>原内容</th>
        </tr>
    </thead>
    <tbody>
    
        <tr class="odd">
            <td>%s</td>
            <td>%s</td>
        </tr>
        
        </tbody>
    </table>
    ''' % (newContent, oldContent)

    # 三个参数：第一个为文本内容，第二个 plain 设置文本格式，第三个 utf-8 设置编码
    message = MIMEText(msg, 'html', 'utf-8')
    message['From'] = Header("爬虫", 'utf-8')
    message['To'] = Header('老大们', 'utf-8')

    subject = "【变化】- " + url
    message['Subject'] = Header(subject, 'utf-8')

    try:

        smtp = smtplib.SMTP()
        smtp.connect(mail_host)
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.set_debuglevel(1)
        smtp.login(mail_user, mail_pass)
        smtp.sendmail(sender, receivers, message.as_string())
        smtp.quit()

        # smtpObj = smtplib.SMTP()
        # smtpObj.connect(mail_host, 465)  # 25 为 SMTP 端口号
        # smtpObj.login(mail_user, mail_pass)
        # smtpObj.sendmail(sender, receivers, message.as_string())
        print "邮件发送成功"
    except smtplib.SMTPException:
        print "Error: 无法发送邮件"

def startFromYml():
    while 1:
        sites = loadYaml("sites.yml")
        for site in sites:
            htmlContent = getQichachaHtml(site)
            if not htmlContent:
                continue
            soup = getSoupByStr(htmlContent)
            for rule in sites[site]:
                tag = soup.select_one(rule)
                tagStr = unicode(tag)
                forTagStr = getFomContentByUrl(site)
                if not forTagStr:
                    InsertContentByUrl(site,tagStr)
                    continue

                sendEmail(tagStr, forTagStr, site)

                if forTagStr != tagStr:
                    sendEmail(tagStr, forTagStr, site)
                else:
                    UpdateContentByUrl(site,tagStr)
        time.sleep(60 * 2)

if __name__ == "__main__":
    # if len(sys.argv)> 1:
    #     inputProvs = sys.argv[1]
    #     provs = []
    # else:
    #     print u'请输入'

    startFromYml()