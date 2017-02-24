##!/usr/bin/python
# -*- coding: UTF-8 -*-

import  yaml

bloomDumpCapsName = 'testBloomDump'
bloomDumpBooks = 'BooksBloomDump'
capListAPIDeviceInfo = '&soft_id=1&ver=110817&platform=an&placeid=1007&imei=862953036746111&cellid=13&lac=-1&sdk=18&wh=720x1280&imsi=460011992901111&msv=3&enc=666501479540451111&sn=1479540459901111&vc=e8f2&mod=M3'

def loadYaml(fileName):
    try:
        conf =yaml.load(file(fileName))
    except Exception as e:
        print e
        return None
    return conf

def loadRules():
    # try:
    #     conf = s=yaml.load(file('rules.yaml'))
    # except Exception as e:
    #     print e
    #     return None
    return loadYaml('rules.yaml')

def loadShuQC():
    return loadYaml('shuqCategory.yaml')

def loadShuQSeqC():
    return loadYaml('shuqCategorySeq.yaml')


def loadIgnores():
    # try:
    #     conf = s=yaml.load(file('ignore.yaml'))
    # except Exception as e:
    #     print e
    #     return None
    return loadYaml('ignore.yaml')

def loadCategory():
    return loadYaml('category.yaml')

def dumpDict2Yaml(fileName,dct):
    f = open(fileName, 'wb')
    yaml.dump(dct, f)
    f.close()

def loadCrawledBook():

    from pybloom import BloomFilter
    f = BloomFilter(capacity=10000000, error_rate=0.001)
    return f
    # [f.add(x) for x in range(10)]
def getBloom(cap=10000000):

    from pybloom import BloomFilter
    f = BloomFilter(capacity=cap, error_rate=0.001)
    return f

def loadBloomFromFile(fileName = bloomDumpCapsName):
    from pybloom import BloomFilter
    try:
        bloom = BloomFilter.fromfile(open(fileName, 'r'))
    except IOError as er:
        print 'load bloom from file fail, return null', er
        return None
    except Exception as e:
        print 'load bloom from file got exception, return null', e
        return None
    return bloom

def loadBloomBooks(fileName = bloomDumpBooks):
    return loadBloomFromFile(fileName)


def dumpBloomToFile(bloom, fileName = bloomDumpCapsName):
    bloom.tofile(open(fileName, 'w'))





# bloom = getBloom(1000*1000)
# for i in range(0, 500):
#     bloom.add(i)
#
# dumpBloomToFile(bloom,)
#
# bl2 = loadBloomFromFile()
# if 300 in bl2:
#     print 'ok'
# else:
#     print 'fail'
#
# if 500 in bl2:
#     print 'fail'
# else:
#     print 'ok'
#
# bl2.add(550)
#
# dumpBloomToFile(bloom,)
#
# if 300 in bl2:
#     print 'ok'
# else:
#     print 'fail'
#
# if 500 in bl2:
#     print 'fail'
# else:
#     print 'ok'
#
# if 550 in bl2:
#     print 'ok'
# else:
#     print 'fail'


    # [f.add(x) for x in range(10)]


