#!/usr/bin/python
# -*- coding: UTF-8 -*-
import smtplib
import time
from email.header import Header
from email.mime.text import MIMEText

from Config import MAILPASS, NO_REPLY_TATATIMES_COM, SMTP_EXMAIL_QQ_COM, ZYQ_TATATIMES_COM, MANAGER_TATATIMES_COM, \
    NOREPLAYEMAIL
from networkHelper import getERAConn, getEADConn

yesteday = time.strftime('%Y%m%d', time.localtime(time.time() - 24 * 3600))
# yesteday = time.strftime('%Y%m%d', time.localtime(time.time()))


def bossconsume():
    csorEAD, connEAD = getEADConn()
    csorERA, connERA = getERAConn()
    #所有广告创意信息
    adObjs = dict()
    #所有广告主信息
    advertisers = dict()
    #所有广告位信息
    adunits = dict()

    # csorERA.excute('select ')
    # 查广告、及所属广告主、及所投广告位名称及媒体名称
    csorERA.execute('select contentId, title, adunitid,advertiserId,advertiser.name,adslot.mapName, adsponsor.name '
                    'from adsdkcontent,advertiser,adslot,adsponsor where adsdkcontent.advertiserId = advertiser.sponsorId '
                    'and adsdkcontent.adunitid = adslot.slotUdid and adsponsor.sponsorId = adslot.sponsorId;')
    connERA.commit()


    contents = csorERA.fetchall()
    for content in contents:
        adObj = dict()
        adObj['adid'] = content[0]
        adObj['title'] = content[1]
        adObj['adunitid'] = content[2]
        adObj['advertiserId'] = content[3]
        adObj['advertiserName'] = content[4]
        adObj['slotName'] = content[5]
        adObj['adsponsorName'] = content[6]

        adObjs[content[0]] = adObj

        if advertisers.has_key(adObj['advertiserId']):
            adids = advertisers.get(adObj['advertiserId'])
            advertisers[adObj['advertiserId']] = adids + ',' + str(adObj['adid'])
        else:
            advertisers[adObj['advertiserId']] = str(adObj['adid'])

    #各广告商各广告位消费
    consums = list()

    # 在各媒体自投广告汇总
    consumOnMedias = dict()

    #所有统计到的广告投放情况信息
    csorEAD.execute('select ad.adId as adId, sum(ad.clickcnt) as clickcnt, sum(ad.showcnt) as showcnt, ad.cdate as cdate'
                   ', sum(acc.amount) as accAmount  from ad_statistic_ad_record ad '
                   'left join ad_accounts_record acc on ad.id = acc.sourceId  '
                   'where ad.sourceId=1 and ad.cdate=%s GROUP BY adId, cdate ORDER BY cdate DESC;', (yesteday, ))
    connEAD.commit()

    stastics = csorEAD.fetchall()
    for static in stastics:
        adid = static[0]
        clickcnt = static[1]
        showcnt = static[2]
        cdate = static[3]

        if not adObjs.has_key(adid):
            continue
        adObj = adObjs[adid]
        adObj['clickcnt'] = clickcnt
        adObj['showcnt'] = showcnt
        adObj['cdate'] = cdate
        consums.append(adObj)

        if not consumOnMedias.has_key(adObj['adsponsorName']):
            consumOnMd = dict()
            consumOnMd['name'] = adObj['adsponsorName']
            consumOnMd['clickcnt'] = clickcnt
            consumOnMd['showcnt'] = showcnt
            consumOnMedias[adObj['adsponsorName']] = consumOnMd
        else:
            consumOnMd = consumOnMedias[adObj['adsponsorName']]

            consumOnMd['clickcnt'] = consumOnMd['clickcnt'] + clickcnt
            consumOnMd['showcnt'] = consumOnMd['showcnt'] + showcnt



    return consumOnMedias, consums

def mediaIncome():
    csorERA, connERA = getERAConn()
    #媒体总收入
    mediaObjs = list()

    csorERA.execute('select c.sponsorId, s.name, sum(c.showNum) as showNum, sum(c.clickNum) as clickNum'
                    ', sum(c.cost) as cost, c.day as day, sum(c.reqNum) as reqNum '
                    ' from adslot_dailycost c left join adsponsor s on c.sponsorId = s.sponsorId'
                    ' left join adslot a on c.slotId = a.slotId where c.day  = %s and showNum > 0 '
                    'Group BY day, a.sponsorId ORDER BY day, a.sponsorId DESC;',  (yesteday, ))
    connERA.commit()


    contents = csorERA.fetchall()
    for content in contents:
        adObj = dict()
        adObj['sponsorId'] = content[0]
        adObj['name'] = content[1]
        adObj['showcnt'] = content[2]
        adObj['clickcnt'] = content[3]
        adObj['cost'] = content[4]
        adObj['day'] = content[5]
        adObj['reqNum'] = content[6]
        # adObj['sloName'] = content[5]

        mediaObjs.append(adObj)

    # 媒体广告位收入详情
    slotIncoms = list()

    #广告位详情
    csorERA.execute('select c.sponsorId, sum(c.showNum) as showNum, sum(c.clickNum) as clickNum, sum(c.cost) as cost'
                   ', c.day as day, sum(c.reqNum) as reqNum, a.mapName, c.slotId as slotId  from adslot_dailycost c'
                   ' left join adslot a on c.slotId = a.slotId where c.day  = %s  '
                   'Group BY day, a.slotId ORDER BY day,c.sponsorId DESC;', (yesteday, ))
    connERA.commit()

    stastics = csorERA.fetchall()
    for static in stastics:
        adObj = dict()
        adObj['sponsorId'] = static[0]
        adObj['showcnt'] = static[1]
        adObj['clickcnt'] = static[2]
        adObj['cost'] = static[3]
        adObj['day'] = static[4]
        adObj['reqNum'] = static[5]
        adObj['slotName'] = static[6]
        adObj['slotId'] = static[7]

        slotIncoms.append(adObj)

    return mediaObjs,slotIncoms

def send(msg):
    mail_host= SMTP_EXMAIL_QQ_COM  #设置服务器
    mail_user= NO_REPLY_TATATIMES_COM  #用户名
    mail_pass= MAILPASS  #口令

    sender = NOREPLAYEMAIL
    receivers = [MANAGER_TATATIMES_COM, ZYQ_TATATIMES_COM]  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱



    # 三个参数：第一个为文本内容，第二个 plain 设置文本格式，第三个 utf-8 设置编码
    message = MIMEText(msg, 'html', 'utf-8')
    message['From'] = Header("广告平台", 'utf-8')
    message['To'] =  Header('老大们', 'utf-8')

    subject = "塔塔日报-" + yesteday
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


def dailyReport():

    msgHead = u'''
    <!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Title</title>


</head>
<body>
<style>

        table {
            background-color: transparent;
            border-collapse: collapse;
            border-spacing: 0;
            box-sizing: border-box;
            display: table;
            border-collapse: separate;
            border-spacing: 2px;
            border-color: grey;
        }
        thead {
            display: table-header-group;
            vertical-align: middle;
            border-color: inherit;
        }

        .table>caption+thead>tr:first-child>th, .table>colgroup+thead>tr:first-child>th, .table>thead:first-child>tr:first-child>th, .table>caption+thead>tr:first-child>td, .table>colgroup+thead>tr:first-child>td, .table>thead:first-child>tr:first-child>td {
            border-top: 0;
        }
         .table thead>tr>th {
            border-bottom: 1px solid #ebebeb;
        }
         .table-striped>thead th {
            background: #f5f5f5;
            border-right: 1px solid #f1f1f1;
        }
         .table td,  .table th {
            padding: 6px 15px;
            border-top: 1px solid #f1f1f1;
        }
        .table>thead>tr>th {
            vertical-align: bottom;
            border-bottom: 2px solid #dddddd;
        }
        .table>thead>tr>th, .table>tbody>tr>th, .table>tfoot>tr>th, .table>thead>tr>td, .table>tbody>tr>td, .table>tfoot>tr>td {
            padding: 8px;
            line-height: 1.428571429;
            vertical-align: top;
            border-top: 1px solid #dddddd;
        }
        th {
            text-align: left;
        }
        *, *:before, *:after {
            -webkit-box-sizing: border-box;
            -moz-box-sizing: border-box;
            box-sizing: border-box;
        }

        th {
            font-weight: bold;
            text-align: -internal-center;
        }
        td, th {
            display: table-cell;
            vertical-align: inherit;
        }
        .text-sm {
            font-size: 12px;
        }
        table {
            border-collapse: collapse;
            border-spacing: 0;
        }
        table {
            display: table;
            border-collapse: separate;
            border-spacing: 2px;
            border-color: grey;
        }
        @media (min-width: 768px)
            .vbox {
                display: table;
                border-spacing: 0;
                position: relative;
                height: 100%;
                width: 100%;
            }

            @media (min-width: 768px)
                .hbox {
                    display: table;
                    table-layout: fixed;
                    border-spacing: 0;
                    width: 100%;
                }

                @media (min-width: 768px)
                    .vbox {
                        display: table;
                        border-spacing: 0;
                        position: relative;
                        height: 100%;
                        width: 100%;
                    }

                    body {
                        font-family: "Open Sans", "Helvetica Neue", Helvetica, Arial, sans-serif;
                        font-size: 13px;
                        color: #717171;
                        background-color: transparent;
                        -webkit-font-smoothing: antialiased;
                    }
                    body {
                        font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
                        font-size: 14px;
                        line-height: 1.428571429;
                        color: #333333;
                        background-color: #ffffff;
                    }

                    html {
                        font-size: 62.5%;
                        -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
                    }
                    html {
                        font-family: sans-serif;
                        -webkit-text-size-adjust: 100%;
                        -ms-text-size-adjust: 100%;
                    }
                }
            }
        }
    </style>
    <br>
    <br>
<h1 style="font-size: 20px;">自投广告消费按媒体汇总</h1>
    <br>
<table class="table table-striped m-b-none text-sm" style="background-color: transparent;border-collapse: collapse;border-spacing: 0;box-sizing: border-box;display: table;border-collapse: separate;border-spacing: 2px;border-color: grey;">
    <thead>
        <tr role="row">
            <th>媒体名称</th>
            <th>总点击</th>
            <th>总展示</th>
            <th>点击率</th>
            <th>日期</th>
        </tr>
    </thead>
    <tbody>
    '''
    bossConsumeOnMDTemp = u'''
        <tr class="odd">
            <td>媒体名称</td>
            <td>总点击</td>
            <td>总展示</td>
            <td>点击率</td>
            <td>日期</td>
        </tr>
        '''
    mediaWholeBase = u'''
        </tbody>
    </table>
    <br>
    <br>
    <h1  style="font-size: 20px;">各媒体总收入</h1>
    <br>
    <table class="table table-striped m-b-none text-sm">
        <thead>
            <tr role="row">
                <th>媒体sponsorId</th>
                <th>媒体名称</th>
                <th>总点击</th>
                <th>总展示</th>
                <th>点击率</th>
                <th>总请求</th>
                <th>填充率</th>
                <th>总收入</th>
                <th>日期</th>
            </tr>
        </thead>
        <tbody>
        '''

    MediaWholeIncomeTemp = u'''
        <tr class="odd">
            <td>媒体sponsorId</td>
            <td>媒体名称</td>
            <td>总点击</td>
            <td>总展示</td>
            <td>点击率</td>
            <td>总请求</td>
            <td>填充率</td>
            <td>总收入</td>
            <td>日期</td>
        </tr>
        '''
    slotConsumeDeatilBase = u'''
    </tbody>
</table>
    <br>
    <br>
<h1 style="font-size: 20px;">自投广告按广告位统计</h1>
    <br>
<table class="table table-striped m-b-none text-sm">
    <thead>
        <tr role="row">
            <th>广告位adId</th>
            <th>广告名称</th>
            <th>广告位</th>
            <th>点击次数</th>
            <th>展示次数</th>
            <th>点击率</th>
            <th>日期</th>
        </tr>
    </thead>
    <tbody>
    '''
    slotConsumeDeatilTemp = u'''
    <tr class="even">
        <td>广告位adId</td>
        <td>广告名称</td>
        <td>广告位</td>
        <td>点击次数</td>
        <td>展示次数</td>
        <td>点击率</td>
        <td>日期</td>
    </tr>
        '''
    slotIncomeDeatilBase = u'''
        </tbody>
    </table>
    <br>
    <br>
    <h1 style="font-size: 20px;">媒体广告位收入详情</h1>
    <br>
    <table class="table table-striped m-b-none text-sm">
        <thead>
            <tr role="row">
                <th>广告位adId</th>
                <th>广告位名称</th>
                <th>点击次数</th>
                <th>展示次数</th>
                <th>点击率</th>
                <th>请求次数</th>
                <th>填充率</th>
                <th>日期</th>
            </tr>
        </thead>
        <tbody>
        '''
    slotIncomeDeatilTemp = u'''
        <tr class="even">
            <td>广告位adId</td>
            <td>广告位名称</td>
            <td>点击次数</td>
            <td>展示次数</td>
            <td>点击率</td>
            <td>请求次数</td>
            <td>填充率</td>
            <td>日期</td>
        </tr>
            '''
    tail = u'''
    </tbody>
</table>
</body>
</html>

    '''

    consumeOnMedias, consums = bossconsume()
    mediaObjs, slotIncoms = mediaIncome()

    mg = msgHead
    # 在各媒体自投总计
    for consumOnMdKey in consumeOnMedias:
        consumOnMd = consumeOnMedias[consumOnMdKey]
        record = bossConsumeOnMDTemp.replace(u'媒体名称', consumOnMd['name']).replace(u'总点击', str(consumOnMd['clickcnt']))\
            .replace(u'总展示', str(consumOnMd['showcnt']))\
            .replace(u'日期', str(yesteday))

        if not consumOnMd.has_key('showcnt') or consumOnMd['showcnt'] < 1:
            record = record.replace(u'点击率', str(0))
        else:
            record = record.replace(u'点击率', str('%.2f' % ((float(consumOnMd['clickcnt']) / float(consumOnMd['showcnt'])) * 100)) + '%')

        mg = mg + record

    mg = mg + mediaWholeBase

    # 各媒体总收入
    for consumOnMd in mediaObjs:
        # consumOnMd = consumeOnMedias[consumOnMdKey]

        record = MediaWholeIncomeTemp.replace(u'媒体sponsorId', str(consumOnMd['sponsorId']))\
            .replace(u'媒体名称', consumOnMd['name'])\
            .replace(u'总点击', str(consumOnMd['clickcnt']))\
            .replace(u'总展示', str(consumOnMd['showcnt']))\
            .replace(u'总请求', str(consumOnMd['reqNum']))\
            .replace(u'总收入', str('%.2f'%(float(consumOnMd['cost']) / 100.0)) + u'元')\
            .replace(u'日期', str(yesteday))

        if not consumOnMd.has_key('showcnt') or consumOnMd['showcnt'] < 1:
            record = record.replace(u'点击率', str(0))
        else:
            record = record.replace(u'点击率', str('%.2f' % ((float(consumOnMd['clickcnt']) / float(consumOnMd['showcnt'])) * 100)) + '%')

        if not consumOnMd.has_key('reqNum') or consumOnMd['reqNum'] < 1:
            record = record.replace(u'填充率', str(0))
        else:
            record = record.replace(u'填充率', str('%.2f' % ((float(consumOnMd['showcnt']) / float(consumOnMd['reqNum'])) * 100)) + '%')

        mg = mg + record

    mg = mg + slotConsumeDeatilBase

#    自投广告在广告位上投放详情
    for consumOnMd in consums:
        # consumOnMd = consumeOnMedias[consumOnMdKey]

        record = slotConsumeDeatilTemp.replace(u'广告位adId', str(consumOnMd['adid']))\
            .replace(u'广告名称', consumOnMd['title'])\
            .replace(u'广告位', consumOnMd['slotName'])\
            .replace(u'点击次数', str(consumOnMd['clickcnt']))\
            .replace(u'展示次数', str(consumOnMd['showcnt']))\
            .replace(u'日期', str(yesteday))
        if not consumOnMd.has_key('showcnt') or consumOnMd['showcnt'] < 1:
            record = record.replace(u'点击率', str(0))
        else:
            record = record.replace(u'点击率', str('%.2f' % ((float(consumOnMd['clickcnt']) / float(consumOnMd['showcnt'])) * 100)) + '%')
        mg = mg + record

    mg = mg + slotIncomeDeatilBase

    #    广告位收入详情
    for consumOnMd in slotIncoms:
        # consumOnMd = consumeOnMedias[consumOnMdKey]
        if consumOnMd['showcnt'] < 1:
            continue

        record = slotIncomeDeatilTemp.replace(u'广告位adId', str(consumOnMd['slotId'])) \
            .replace(u'广告位名称', consumOnMd['slotName']) \
            .replace(u'点击次数', str(consumOnMd['clickcnt'])) \
            .replace(u'展示次数', str(consumOnMd['showcnt'])) \
            .replace(u'请求次数', str(consumOnMd['reqNum'])) \
            .replace(u'日期', str(yesteday))


        if not consumOnMd.has_key('showcnt') or consumOnMd['showcnt'] < 1:
            record = record.replace(u'点击率', str(0))
        else:
            record = record.replace(u'点击率', str('%.2f' % ((float(consumOnMd['clickcnt']) / float(consumOnMd['showcnt'])) * 100)) + '%')

        if not consumOnMd.has_key('reqNum') or consumOnMd['reqNum'] < 1:
            record = record.replace(u'填充率', str(0))
        else:
            record = record.replace(u'填充率', str('%.2f' % ((float(consumOnMd['showcnt']) / float(consumOnMd['reqNum'])) * 100)) + '%')


        mg = mg + record

    mg = mg + tail

    send(mg)

if __name__ == '__main__':
    dailyReport()
    # produceBookSuggest()