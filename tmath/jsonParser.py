#!/usr/bin/python
# -*- coding: UTF-8 -*-
from dbHelper import getConn
from networkHelper import getContent
import json

def baiduInt():
    base = 'https://sp0.baidu.com/9_Q4sjW91Qh3otqbppnN2DJv/pae/channel/data/asyncqury?from_mid=1&format=json&ie=utf-8&oe=utf-8&subtitle=%E8%84%91%E7%AD%8B%E6%80%A5%E8%BD%AC%E5%BC%AF&Cookie=46FEC0BEEC699F19604419952C6908F2&query=%E8%84%91%E7%AD%8B%E6%80%A5%E8%BD%AC%E5%BC%AF&rn=8&stat0=%E7%9B%8A%E6%99%BA&pn='
    base2 = '&srcid=4030&appid=4030&cb=jQuery1102040624547459713845_1476168433602&_=1476168433666'

    csor,conn = getConn()

    for i in range(0, 50):
        print '------------------i = ',i
        url = base + str(8 * i) + base2
        content = getContent(url)
        print content[43:-1]
        int = json.loads(content[43:-1])

        for data in int['data'][0]['disp_data']:
            type = data['stat0']
            author = data['author']
            name = data['ename']
            content = name
            sql = "INSERT ignore INTO daily(name, \
                                                    type, content,stage, gred,tag, author) \
                                                    VALUES ('%s', '%d', '%s', '%s', '%d', '%s', '%s')" % \
                  (name, 4, content, '1,2,3,4', 0, type, author)
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





if __name__ == '__main__':
    baiduInt()