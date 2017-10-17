# -*- coding: utf-8 -*-
'''
@ author: liyang
@ time: 2017/8/17
@民意分析
'''
import json
import logging
import logging.config
from opinion import *

import re

def initDictType():
    regex = re.compile(r'\\')
    json_string=open("F:\\esoft\\xinfang\\other\\json.json","r")
    json_string = regex.sub(r"\\\\", json_string.read())
    jsonObject = json.loads(json_string)

    for js in jsonObject.get("all") :
        # 第一级节点 违法
        if(js.get("leaf")):
           initDictData(js)
        else :
           initDictData(js)
           #第一级节点 违纪
           for jsOne in js.get("children"):
              # #第二级节点 违纪
              if(jsOne.get("leaf")):
                 initDictData(jsOne)

import os
# 初始化词文件
def initDictFile(cursor):
    allPath = ''
    # 获取词典信息
    dictDf = getDictInfo(cursor)
    # 新建文件
    for index, row in dictDf.iterrows():
        # 新建文件夹
        srcUrl = row['CLASS_WORD_DICT_URL']
        dePa = srcUrl.split("\\")
        path1 = srcUrl[0:len(srcUrl) - len(dePa[len(dePa) - 1]) - 2]
        allPath = path1
        if not (os.path.exists(path1)):
            os.makedirs(path1)
        with open(srcUrl, 'a') as wf2:  # 打开文件
            wf2.closed

    # 获取词
    wordDf = addDictFile (cursor)
    for index, row in wordDf.iterrows():
        url = allPath + "\\" + row['PARENT_CODE'] + ".txt"
        print url
        with open(url, 'a') as wf2:  # 打开文件
             wf2.write('\n')  # 写入txt文档
             wf2.write(str(row['WORD']).encode('utf-8') + ' ' +  str(row['WORD_FREQUENCY']).encode('utf-8')+ ' '+ 'n'.encode('utf-8'))  # 写入txt文档
             wf2.closed


    # path = "F:\\esoft\\xinfang\\other\dd\\json.txt"
    # dePa = path.split("\\")
    # print dePa
    # path1 = path[0:len(path) - len(dePa[len(dePa)-1]) - 2]
    # print  path1
    # if not (os.path.exists("F:\\esoft\\xinfang\\other\\dd\\kk\\dd")):
    #     os.makedirs("F:\\esoft\\xinfang\\other\\dd\\kk\\dd")
    #     with open("F:\\esoft\\xinfang\\other\dd\\json.txt", 'a') as wf2:  # 打开文件
    #         wf2.write('\n')  # 写入txt文档

            # wf2.write(word + ' ' + wordFrequency + ' ' + wordPart)  # 写入txt文档
            # wf2.closed
        # print fp
        # print fp.read()


def test() :
  print

if __name__ == '__main__':
    test()