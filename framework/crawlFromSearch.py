##!/usr/bin/python
# -*- coding: UTF-8 -*-
import random
import traceback

import time

import requests

from dbHelper import getDushuConnPool

moveCapcarry = 5000
moveBookcarry = 40



if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        moveCapcarry = int(sys.argv[1])
        moveBookcarry = int(sys.argv[2])

    # dailyMove()
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
