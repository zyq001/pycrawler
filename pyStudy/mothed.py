#!/usr/bin/python
# -*- coding: UTF-8 -*-

def methodCall(x,l=[]):
    for i in range(x):
        l.append(i * i)
    print l

if __name__ == "__main__":
    methodCall(2)
    methodCall(3,[3, 2, 1])
    methodCall(3)