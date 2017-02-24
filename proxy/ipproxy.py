#!/usr/bin/python
# -*- coding: UTF-8 -*-
import json

import requests

from Config import ipProxyServerUrl


def getAvailableIPs(count=-1,types=0,country=u'国内', area=''):
    if count != -1:
        url = ipProxyServerUrl + "?count=" + str(count) + '&types=' + str(types) + '&country=' + country
    else:
        url = ipProxyServerUrl + '?types=' + str(types) + '&country=' + country

    if area != '':
        url = url + '&area=' + area

    r = requests.get(url)
    ip_ports = json.loads(r.text)

    return ip_ports


def deletByIP(ip):
    url = ipProxyServerUrl + 'delete?ip=' + ip
    r = requests.get(url)
    print 'delete proxy ip, ',ip, ', resp:',r.text