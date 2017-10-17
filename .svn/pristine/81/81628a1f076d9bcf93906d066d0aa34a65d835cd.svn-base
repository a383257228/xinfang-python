
# -*- coding: utf-8 -*-
'''
@ author: liyang
@ time: 2017/8/17
@民意分析 分词
'''
from sklearn.feature_extraction.text import CountVectorizer
from commonUtil import getLog # 从文件路径为connectionDB2的文件中导入日志
from numpy import *
import operator
import os
from opinion import getDictInfo
from opinion import getDfContent
from opinion import updateOpinionOldWord
from opinion import updateOpinionWord
from opinion import updateSettCT
from commonUtil import dec #解密
import jieba
import jieba.posseg as pseg
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )
# 获取日志对象
logging = getLog()
'''
分词初始化 取文本 分词 更新词典
'''
def participleInit(conn,cursor,start,end,times,dictDf,rowAll) :
        try:
            # 分类词典
            classDict = []
            for index, row in dictDf.iterrows():   # 获取每行的index、row
                classDict.append(row['CLASS_WORD_DICT_URL'])

            # 停止词路径
            stopword = dictDf.iat[0, 0]
            #过滤停止词
            stoplist = {}.fromkeys([ line.strip() for line in open(stopword) ])

            # 获取分词文本
            conDf = getDfContent(cursor,start,end)
            # print conDf
            if conDf is not None:

               for index, row in conDf.iterrows():   # 获取每行的index、row
                logging.info("第" + str(times) + "轮: 第" + str(index + 1 + rowAll) + "条" + ": fenci Start!")
                news = ''
                # 扫描文本不存在
                if(row['FILE_CONTENT'] != 'None' and row['FILE_CONTENT'] is not None and str(row['FILE_CONTENT']) != ''):
                    news = str(row['FILE_CONTENT']).encode("utf-8")
                else :
                        if(row['IIC'] != 'None' and row['IIC'] != None and str(row['IIC']).encode("utf-8") != '') :
                            news = str(row['IIC']).encode("utf-8")
                        else :
                            if (row['CIC'] != 'None' and row['CIC'] != None and (row['CIC']) != ''):
                                news = str(row['CIC']).encode("utf-8")
                # print news
                # 进行分词等操作并更新数据库
                try:
                    if(news != '') :
                       rowAlls = {}
                       rowAlls.setdefault("PETITION_BASIC_INFO_OID", row['OID'])
                       rowAlls.setdefault("ISSUE_REGION_FLAG", row['ISSUE_REGION_FLAG'])
                       rowAlls.setdefault("REGION_CODE", row['REGION_CODE'])
                       rowAlls.setdefault("REGION_NAME", row['REGION_NAME'])
                       rowAlls.setdefault("ISSUE_REGION_CODE", row['ISSUE_REGION_CODE'])
                       rowAlls.setdefault("ISSUE_REGION_NAME", row['ISSUE_REGION_NAME'])

                       news =  dec(news)
                       updateWordsDB(cursor, news, stoplist, classDict,rowAlls)
                       updateSettCT(cursor,index + 1)
                       logging.info("第" + str(times) + "轮: 第" + str(index + 1 + rowAll) + "条" + ": fenci Success!")
                except Exception as ex:
                    logging.error("news Failed: %s\t" % (ex))
                   # print ("第" + str(times) + "轮: 第" + str(index + 1) + "条" + ": fenci Success!")

            else :
                logging.warning("getDfContent:\t Context is empty.")
            # try:
            #     aa = None
            #     aa[0][0:1]
            # except Exception as e:
            #     cursor.close()
            #     conn.rollback()
            #     # conn.commit()
            #     conn.close()
        except Exception as ex:
            logging.error("participleInit Failed: %s\t" % (ex))

'''
进行分词等操作并更新数据库
'''
def updateWordsDB(cursor,news,stoplist,classDict,rowAll):
    arrCount = countWord(news, stoplist)
    if(arrCount is not None) :
        dictStr, oldStr = updateDict(arrCount, classDict)
        # 新词暂存到数据库
        if dictStr != '':
            updateOpinionWord(cursor, dictStr,rowAll)
        # 旧词更新词频使用
        if oldStr != '':
            updateOpinionOldWord(cursor, oldStr)


'''
更新词典 找出新词
'''
def updateDict(arrCount,classDict):
    logging.info(arrCount)
    arrEnd = arrCount
    # 记录过滤后的数据
    arrNew = []
    # # 记录最后剩的新词
    count = 0
    arrOld = [] #保存词典中已有的词  用来更新词频
    # 根据分类词典 过滤词
    for i in classDict:
        count += 1
        # 判断文件是否存在
        tag = os.path.exists(i)
        # 新文件
        if not tag:
            flag = 1
            dictlist = {}
            # 2.对比词典
        else:
            flag = 0
            dictlist = {}.fromkeys([line.strip().split(" ")[0] for line in open(i)])

        if flag == 0:
            if count != 1:
                arrEnd = arrNew
                arrNew = []

            for word in arrEnd:
                # print word[0].split("_")[0].encode('utf-8')
                # logging.info(word)
                if word[0].split("_")[0].encode('utf-8') not in dictlist:
                    arrNew.append(word)
                    # print word[0].split("_")[0].encode('utf-8')

                else :
                    # 词典里有该词 则保存 用来更新数据库中的词频
                    # print word[0].split("_")[0].encode('utf-8')
                    arrOld.append(word)

    nvDict = dict((name, value) for name, value in arrNew)
    sortarr = sorted(nvDict.iteritems(), key=operator.itemgetter(1), reverse=True)
    dictStr = ''
    logging.info(sortarr)
    for i in range(0,len(sortarr)):
        if(len(sortarr[i]) > 1) :
            # if sortarr[i][1] > 1:
            if(len(sortarr[i][0].split("_")) > 1) :
               dictStr += sortarr[i][0].split("_")[0] + " " + sortarr[i][0].split("_")[1] + " " + str(sortarr[i][1]) + ","
    nvDictOld = dict((name, value) for name, value in arrOld)
    sortarrOld = sorted(nvDictOld.iteritems(), key=operator.itemgetter(1), reverse=True)
    logging.info(sortarrOld)
    oldStr = ''
    for i in range(0,len(sortarrOld)):
        # if sortarr[i][1] > 1:
        oldStr += sortarrOld[i][0].split("_")[0] + " " + sortarrOld[i][0].split("_")[1] + " " + str(sortarrOld[i][1]) + ","
    return dictStr,oldStr

'''
分词  统计词频
'''
def countWord(news,stoplist):
    try :
        seg_lists = pseg.cut(news)
        segsEnd = ""
        for w in seg_lists:
            # 过滤一个词的数据
            if(w.word != u'') :

                if len(w.word.encode('utf-8')) > 3 and w.word.encode('utf-8') not in stoplist and w.word != " ":
                    if (w.flag != "nr" and w.flag != "nr1"
                        and w.flag != "nr2" and w.flag != "nrj" and w.flag != "nrf"
                        and w.flag != "ns" and w.flag != "nsf" and w.flag != "nt"
                        and w.flag != "nz" and w.flag != "m"):
                        #segsEnd += str(w.word + "_" + w.flag + " ").encode('utf-8')
                        segsEnd += str(w.word + "_" + "n" + " ").encode('utf-8')
        if(segsEnd != "") :
            segs = [segsEnd]
            # segs.append(starEnd)

            cv = CountVectorizer()
            cv_fit = cv.fit_transform(segs)
            cvArr = cv.get_feature_names()
            arr = cv_fit.toarray()
            # print arr
            arrCount = []
            # 累计词频
            for arrI in arr:
                nvs = zip(cvArr, arrI)
                # print nvs
                if len(arrCount) == 0:
                    arrCount = nvs
                else:
                    for i in range(0, len(arrCount)):
                        arrCount[i] = (str(arrCount[i][0]).encode('utf-8'), arrCount[i][1] + nvs[i][1])
            return arrCount
        else :
           return None
    except Exception as ex:
        logging.error("countWord : %s\t" % (ex))
        return None


def participle(conn,cursor,start,end,times,rowAll) :
    try:
        logging.info("Start participle!")
        try:
            dictDf = getDictInfo(cursor)
        except Exception as ex:
            logging.error("getDictInfo: %s\t" % (ex))
        if dictDf is not None:
            if(times == 1) :
                # 自定义词典路径
                userDict = dictDf.iat[0, 1]
                # 导入词典、停用词
                # 词典文件是utf8编码
                jieba.load_userdict(userDict)  # file_name为自定义词典的路径
                # jieba.load_userdict("F:\\esoft\\xinfang\\other\\dict\\012101.txt")

                for index, row in dictDf.iterrows():  # 获取每行的index、row
                    dictUrl = row['CLASS_WORD_DICT_URL']
                    # 判断字典路径是否存在
                    tag = os.path.exists(dictUrl)
                    if not tag :
                        with open(dictUrl, 'a') as wf2:  # 打开文件
                             wf2.write('\n')  # 写入txt文档
                             wf2.closed
                    jieba.load_userdict(dictUrl)  # file_name为自定义词典的路径
                logging.info("import fenci  dict Success!")


            participleInit(conn,cursor,start,end,times,dictDf,rowAll)
            return "success"
        logging.info("End participle!")

        # print(u'\n民意分析分词并更新数据库成功，用时：%0.2f秒' % (endTime - startTime)) # 打印运行总时间
    except Exception as e:
        logging.error( e)
        return "Falied"
    finally:
        pass