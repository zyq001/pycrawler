#!/usr/bin/python
# -*- coding: utf8 -*-
import json
import codecs
from collections import OrderedDict


def testNewDict():
    aliSearchUrl = "http://60.205.57.230:44406/patent3/_search"
    aliAnaUrl = 'http://60.205.57.230:44406/patent/_analyze?analyzer=ik_smart'
    xianAnaUrl = 'http://10.0.0.15:8600/patent/_analyze?analyzer=ik_smart'
    sentences = []
    result = list()
    # for page in range(0,30)
    field = 'abso'
    query = '''{
            "query": {
                "match_phrase": {
                    "abso": "计算机"
                }
              },
              "_source":["%s"],
              "from":0,
              "size":300
            }'''% field
    import requests

    resp = requests.post(aliSearchUrl, data=query)
    respJson = json.loads(resp.text)
    [sentences.append(x['_source'][field]) for x in respJson['hits']['hits']]

    outF = codecs.open('dict-test3.json','w','utf-8')

    for sent in sentences:
        befResp = requests.post(aliAnaUrl, data=sent.encode('utf-8'))
        aliWords = list()
        [aliWords.append(x['token'].encode('utf-8')) for x in json.loads(befResp.text)['tokens']]
        # [aliWords.add(x['token'].encode('utf-8')) for x in json.loads(befResp.text)['tokens']]
        aftResp = requests.post(xianAnaUrl, data=sent.encode('utf-8'))
        xianWords = list()
        # [xianWords.add(x['token'].encode('utf-8')) for x in json.loads(aftResp.text)['tokens']]
        [xianWords.append(x['token'].encode('utf-8')) for x in json.loads(aftResp.text)['tokens']]
        drops = list()
        for token in aliWords:
            if token not in xianWords:
                drops.append(token)

        mores = list()
        for token in xianWords:
            if token not in aliWords:
                mores.append(token)

        recd = OrderedDict()
        recd['input'] = sent.encode('utf-8')
        recd['400w-dict-tokens'] = aliWords
        recd['1200w-dict-tokens'] = xianWords
        recd['drops'] = drops
        recd['mores'] = mores
        ss1 = unicode(recd)
        # ss = json.dumps(recd, ensure_ascii=False, encoding='utf-8')
        # outF.write(ss.decode('utf-8'))
        # outF.write(str(recd))
        result.append(recd)
    ss = json.dumps(result, ensure_ascii=False, encoding='utf-8')
    outF.write(ss.decode('utf-8'))
    outF.flush()
    outF.close()

    # json.dump(result,outF)
# if __main__ == "__main__":

f = open('test.json','w')
# f.write(u'中华人民共和')
s = u'中华人民共和'.encode('utf-8')
dd = {'test':u'中国'.encode('utf-8')}
f.write(json.dumps(dd, ensure_ascii=False))
f.close()
# import sys
# reload(sys)
# sys.setdefaultencoding('utf-8')
testNewDict()