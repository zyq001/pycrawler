#!/usr/bin/python
# -*- coding: UTF-8 -*-
import json

import oss2

def getBucket():


    return getBucketByName('dushu-content')

def getBucketByName(name):
    # endpoint = OSSINTERNALENDPOINT  # 假设你的Bucket处于杭州区域
    endpoint = OSSINTERNALENDPOINT  # 假设你的Bucket处于杭州区域

    auth = oss2.Auth(OSSUSER, OSSPASSWD)
    bucket = oss2.Bucket(auth, endpoint, name)
    return bucket

def getImgBucket():
    return getBucketByName('tata-img')

def getInfoBucket():
    return getBucketByName('dushu-info')

def upload2Bucket(bucket, id, obj):

    try:
        bucket.put_object(id, obj)
        print 'succ upload ',id
    except Exception as e:
        print id, ' upload faild ',e


def upload2BucketFromJsonFile(bucket, id, jsonFile):

    try:

        bucket.put_object(id, open(jsonFile, 'r').read())
        print 'succ upload ',id
    except Exception as e:
        print id, ' upload faild ',e

