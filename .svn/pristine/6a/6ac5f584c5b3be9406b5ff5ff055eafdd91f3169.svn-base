
# -*- coding: utf-8 -*-
'''
@ author: liyang
@ time: 2017/8/17
@民意分析 处理页面反馈信息
'''
from numpy import *
from opinion import getWordInfo
from opinion import getDictInfo
from opinion import updateDealFlag
from commonUtil import getLog # 从文件路径为connectionDB2的文件中导入所有的函数
import time
import os
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

# 获取日志对象
logging = getLog()

def saveDictInit(cursor):
    # 1.查询数据库里的词
    dealUuid = []
    # 获取词列表
    try:
        wordDf = getWordInfo(cursor)
    except Exception as ex:
        logging.error("getWordInfo Failed : %s\t" % (ex))

    # 获取词典列表
    try:
        dictDf = getDictInfo(cursor)
    except Exception as ex:
        logging.error("getDictInfo Failed : %s\t" % (ex))

    # 词典列表转成json
    classDict = {}
    if dictDf is not None :
       try:
         for index, row in dictDf.iterrows():   # 获取每行的index、row
            classDict.setdefault(row['CODE'],row['CLASS_WORD_DICT_URL'])
       except Exception as ex:
        logging.error("dtct to classDict Failed : %s\t" % (ex))
       # 自定义词典路径
       userDict = dictDf.iat[0,1]
       # 停止词路径
       stopword = dictDf.iat[0,0]

       if wordDf is not None :
           for index, row in wordDf.iterrows():   # 获取每行的index、row
            #过滤停用词
            if(row['IGNORE_FLAG'] == '1'):
               # 保存id
               dealUuid.append(row['OID'])
               # 保存到停用词词典
               with open(stopword,'a') as wf2: #打开文件
                    word = row['NEW_WORD'].encode('utf-8')
                    wf2.write(word+'\n') #写入txt文档
                    wf2.closed
            else :
                if(row['DICTIONARY_FLAG'] != '0'):
                    # 保存id
                    dealUuid.append(row['OID'])
                    # 保存到分类词典中
                    # 获取词典编号 路径
                    uul = str(row['DICTIONARY_FLAG'])
                    dictUrl = classDict.get(uul)
                    if(dictUrl != None) :
                        try:
                            # 保存到分类词典
                            with open(dictUrl,'a') as wf3: #打开文件
                                wf3.write('\n') #写入txt文档
                                word = row['NEW_WORD'].encode('utf-8')
                                wordFrequency = str(row['WORD_FREQUENCY']).encode('utf-8')
                                wordPart  = row['WORD_PART'].encode('utf-8')
                                wf3.write(word + ' ' + wordFrequency + ' ' + wordPart) #写入txt文档
                                wf3.closed
                        except Exception as ex:
                            logging.error("save to classDict Failed : %s\t" % (ex))
                            wf3.closed
                else :
                    # 保存id
                    dealUuid.append(row['OID'])
                    # 如果词修改了 则更新到词典
                    if(row['NEW_WORD'] != row['EDIT_WORD']):
                    # 保存到分词词典
                        try:
                            with open(userDict,'a') as wf4: #打开文件
                                wf4.write('\n') #写入txt文档
                                word = row['NEW_WORD'].encode('utf-8')
                                wordFrequency = str(row['WORD_FREQUENCY']).encode('utf-8')
                                wordPart  = row['WORD_PART'].encode('utf-8')
                                wf4.write(word + ' ' + wordFrequency + ' ' + wordPart) #写入txt文档
                                wf4.closed
                        except Exception as ex:
                            logging.error("save to userDict Failed : %s\t" % (ex))
                            wf4.closed
           logging.info("save word to allDict Success!")

       # if len(dealUuid) > 0:
       #     try:
       #      #更新数据库 更新词状态为处理
       #      updateDealFlag(cursor,dealUuid)
       #      logging.info("updateDealFlag Success!")
       #     except Exception as ex:
       #       logging.error("save to userDict Failed : %s\t" % (ex))

    return dealUuid

def saveDict(cursor):
    try:
        start = time.clock()
        logging.info("Start saveDictInit!")
        dealUuid = saveDictInit(cursor)
        return dealUuid
        end = time.clock()  # 运行结束时间
        logging.info("End saveDictInit!")
        # logging.info(u'\n民意分析保存新词到词典成功，用时：%0.2f秒' % (end - start)) # 打印运行总时间
    except Exception as e:
        logging.error( e) # 打印异常
    finally:
        # cursor.close()  # 关闭连接
        pass