##!/usr/bin/python
# -*- coding: UTF-8 -*-

import  yaml


def loadYaml(fileName):
    try:
        conf =yaml.load(file(fileName))
    except Exception as e:
        print e
        return None
    return conf


def loadFieldAlias():
    return loadYaml('fieldAlias.yaml')

def loadIsArray():
    return loadYaml('isArray.yaml')



def loadCrawledBook():

    from pybloom import BloomFilter
    f = BloomFilter(capacity=1000, error_rate=0.001)
    [f.add(x) for x in range(10)]