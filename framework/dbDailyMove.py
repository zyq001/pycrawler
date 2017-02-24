##!/usr/bin/python
# -*- coding: UTF-8 -*-
import random
import traceback

import time

import requests

from dbHelper import getDushuConnPool

moveCapcarry = 5000
moveBookcarry = 40

def dailyMove():

    conn, csor = getDushuConnPool()

    csor.execute('select id from cn_dushu_acticle where id < 50000000 ORDER  by id desc limit 1')
    conn.commit()

    lastId = csor.fetchone()[0]


    csor.execute('select id from cn_dushu_acticle_temp ORDER  by id desc limit 1')
    conn.commit()

    nowId = csor.fetchone()[0]

    if nowId > lastId + moveCapcarry:
        nowId = lastId + moveCapcarry

    print 'move caps from ',lastId,' to ', nowId

    try:
        csor.execute('insert IGNORE cn_dushu_acticle (SELECT * from cn_dushu_acticle_temp where id >= %s and id <= %s)', (lastId, nowId))
        conn.commit()
    except Exception as e:
        print 'move cap exception, ',traceback.format_exc()


    csor.execute('select id from cn_dushu_book where id < 2000000 ORDER  by id desc limit 1')
    conn.commit()

    blastId = csor.fetchone()[0]


    csor.execute('select id from cn_dushu_book_temp ORDER  by id desc limit 1')
    conn.commit()

    bnowId = csor.fetchone()[0]

    if bnowId > blastId + moveBookcarry:
        bnowId = blastId + moveBookcarry

    print 'move books from ',blastId,' to ', bnowId
    try:
        # csor.execute('insert IGNORE cn_dushu_book (SELECT * from cn_dushu_book_temp where id >= %s and id <= %s)', (421056, 441056))
        csor.execute('insert IGNORE cn_dushu_book (SELECT * from cn_dushu_book_temp where id >= %s and id <= %s and chapterNum > 8)', (blastId, bnowId))
        conn.commit()
    except Exception as e:
        print 'move book exception, ',traceback.format_exc()


    # 补充修正Boost
    # try:
    #     csor.execute('update cn_dushu_book set typeBoost = updateTime where id >= %s and id <= %s', (blastId, bnowId))
    #     conn.commit()
    # except Exception as e:
    #     print 'update boost exception, ',traceback.format_exc()

    # 补充修正size
    try:
        csor.execute('update cn_dushu_book a INNER join (SELECT bookId, sum(size) as sumsize from cn_dushu_acticle '
                     'where bookId >= %s and bookId <= %s GROUP BY bookId) b on b.bookId = a.id and a.id >= %s set a.size = sumsize', (blastId, bnowId, blastId))
        conn.commit()
    except Exception as e:
        print 'update size exception, ',traceback.format_exc()


def produceBookSuggest():
    connERA,csorERA = getDushuConnPool()


    csorERA.execute('select id from cn_dushu_book')
    connERA.commit()

    contents = csorERA.fetchall()
    startTime = time.time()
    for count in range(0,len(contents)):
        book = contents[count]
        bookId = book[0]
        befTime = time.time()
        r = requests.get('http://dushu.tatatimes.com/api/bookSuggest?id=' + str(bookId))
        # print r.text
        endTime = time.time()
        if len(r.text) < 10:
            print r.text
        # time.sleep(random.random())
        print 'this spent:' , str(endTime - befTime), ' totle ' + str(count) +' avg：' ,str((endTime - startTime)/float(count + 1))



if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        moveCapcarry = int(sys.argv[1])
        moveBookcarry = int(sys.argv[2])

    dailyMove()
    # sleepTime = 60 * 10
    # print 'begin sleep ',sleepTime, ' seconds'
    # time.sleep(sleepTime)
    # print 'end sleep'
    # dailyMove()
    # print 'daily move done!!'
    # produceBookSuggest()
    # print 're produceBookSuggest done'
    # print 'begin sleep ',sleepTime, ' seconds'
    # time.sleep(sleepTime)
    # print 'end sleep'
    # dailyMove()
