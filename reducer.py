#!/usr/bin/env python

import sys
import string
import re
import csv
import heapq
currentkey = None

dismap = []
dthmap = {}
distance = 0
index = 0
for line in sys.stdin:
    line = line.strip(',')
    key, value = line.split('\t', 1)
    key = key.split(',')
    value = value.split(',')
    if(key == currentkey):
        for i in range(len(key)):
            distance += float(distance) + (float(key[i]) - float(value[i]))**2
        dismap.append(distance)
        distance = 0
    else:
        if(currentkey):
            dismap.sort()
            dthmap[index] = dismap[200]
            index+=1
            distance = 0
            dismap = []
            currentkey = key
        currentkey = key
        distance = 0
        for i in range(len(key)):
            distance += float(distance) + (float(key[i]) - float(value[i]))**2
        distance = 0
        dismap.append(distance)

dismap.sort()
dthmap[index] = dismap[200]


print(heapq.nlargest(10, dthmap, key=dthmap.get))