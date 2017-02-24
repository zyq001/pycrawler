#!/usr/bin/python
# -*- coding: UTF-8 -*-

import redis
# r = redis.Redis(host='localhost',port=6379,db=0)
r = redis.Redis(host='10.10.1.9',port=6379,db=0)

print r.keys()
keys = r.keys()
for key in r.keys():
    keyType =  r.type(key)
    if keyType != 'hash':
        continue
    value = r.hgetall(key)
    for record in value:
        if record.startswith('api_') and (not record.startswith('api_ids')):
            # if key.endswith('df56'):
            r.hset(key, record, 0)
            print 'reset key ', key, ' count from ',r.hget(key, record), ' to 0'
