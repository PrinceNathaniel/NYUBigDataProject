#!/usr/bin/env python
import sys
import string
import re
import csv

temp_stdin = []

def toStringFormat(src_str):
    res_str = ''
    for i in range(len(src_str)):
        res_str += ','
        res_str += src_str[i]
    return res_str[1:]

for line in sys.stdin:
    entry = csv.reader([line], delimiter='\t')
    entry = list(entry)[0]
    if(entry[0] != ''):
        temp_stdin.append(entry)
    

for key in temp_stdin:
    key = toStringFormat(key)
    for value in temp_stdin:
        value = toStringFormat(value)
        if key!=value:
            print('{0:s}\t{1:s}'.format(key, value))
