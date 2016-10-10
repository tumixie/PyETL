#! python2.7
#-*- coding: utf-8 -*-

'''
判断文件或者dataframe数据的编码
'''

import os
import sys
import re

from chardet.universaldetector import UniversalDetector
import pandas as pd
import chardet


def judge_code(infile, method=1):
    ''' judge encoding of file or string or list.
    Parameter
    ---------------------------
    infile : filename or string or list
    method : `0` or `1` the way of judge method, default 1.
    
    return:  code          str
    
    大文件建议method=1
    
    '''

    if isinstance(infile, list):
        string = ''.join(infile)
        code = chardet.detect(string)['encoding']
    elif isinstance(infile, str):
        if os.path.isfile(infile):
            with open(infile, 'rb') as f:
                if method == 0:
                    content = f.read()
                    code = chardet.detect(content)['encoding']
                elif method == 1:
                    detector = UniversalDetector()
                    ii = 0
                    while True:
                        chunk_data = f.read(1024)
                        detector.feed(chunk_data)
                        ii += 1
                        if detector.done:
                            break
                        if ii >= 10:
                            break
                    detector.close()
                    result = detector.result
                    code = result['encoding']
                else:
                    raise ValueError('illeagal method value.')
        else:
            code = chardet.detect(infile)['encoding']
    elif isinstance(infile, str):
        code = chardet.detect(infile)['encoding']

    if re.search(r'gb\d+', code, re.IGNORECASE):
        return 'gbk'

    return code

if __name__ == '__main__':
    #code = judge_code('a.csv', method=1)
    #code = judge_code('abc我一')
    #code = judge_code(['我','是','一','个'])
    #print code
    #print chardet.detect(u'我是')
    #print isinstance(u'我是', unicode)
    print unicode(u'我是', 'unicode')






