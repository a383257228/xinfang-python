#-*- coding: utf-8 -*-

from saveDict import saveDict
from participle import participle
from opinion import deleteWord
from opinion import deleteOldWord
from opinion import updateWordFrequency
from opinion import updateOpiAnaly
from opinion import getAllContextNum
from opinion import getDealFlag
from opinion import saveDealUuid
from opinion import deleteSettAll
from opinion import getNowMess
from opinion import updateSettCR
from opinion import updateSettDealFlag
from opinion import getdealUuid

from commonUtil import getConfInt # 从文件路径为connectionDB2的文件中导入所有的函数
from commonUtil import getLog # 从文件路径为connectionDB2的文件中导入所有的函数
from commonUtil import db
from commonUtil import connection

import os
import sys
import time
# 获取日志对象
logging = getLog()

def init():
    logging.info('民意分析开始...')
    status = ""
    # 保存新词到词典里
    database, hostname, port, user, passwd, modelCycle, supportCity, supportArea, supportWei = db()
    cursorS, connS = connection(database, hostname, port, user, passwd)  # 连接数据库

    logging.info('民意分析--保存到词典成功连接数据库')
    path = os.path.realpath(sys.argv[0])
    # 每次处理的条数
    onceNum = getConfInt("opinion", "onceNum", path)
    # 查询是否是新的一次
    flag = getDealFlag(cursorS)
    if(flag) :
        # 清空配置表
        deleteSettAll(cursorS)
        # 总条数
        allNumDf = getAllContextNum(cursorS)
        logging.info("allNum : " + str(allNumDf))

        if allNumDf is not None:
            allNum = allNumDf.iat[0, 0]
            allRound = allNum/onceNum
            if(allNum%onceNum > 0) :
                allRound += 1


        # raise
        # 保存新词到词典里
        dealUuid = saveDict(cursorS)
        print dealUuid
        # 删除暂存词的列表
        deleteWord(cursorS)
        deleteOldWord(cursorS)
        #后面要用到的id
        saveDealUuid(cursorS,dealUuid,allRound,onceNum)


    # 获去目前运行到第几轮
    nowDFStr = getNowMess(cursorS)

    curr_strip = 0
    once_strip = 0
    addFlag = False
    if (not flag):
        for index, row in nowDFStr.iterrows():  # 获取每行的index、row
            curr_strip = int(row['CURR_STRIP'])
            once_strip = int(row['ONCE_STRIP'])

        #上次运行的条数
        add_strip = once_strip - curr_strip
        # 第一次运行从上次运行的条数开始
        if(add_strip > 0) :
           addFlag = True
        else :
           addFlag = False
    cursorS.close()  # 关闭连接
    connS.close()
    logging.info('民意分析--保存到词典成功断开数据库')

    flag = True
    while(flag) :
        cursor, conn = connection(database, hostname, port, user, passwd)  # 连接数据库
        # 获去目前运行到第几轮
        nowDF = getNowMess(cursor)
        if nowDF is not None:
            curr_round = 0
            all_round = 0

            for index, row in nowDF.iterrows():  # 获取每行的index、row
                 all_round = int(row['ALL_ROUND'])
                 curr_round  = int(row['CURR_ROUND'])


            if(curr_round != all_round) :
               # 开始条数
               cur_st = curr_round * onceNum
               #  中断运行 第一次开始标识
               addSta = False
               if (addFlag):
                    # 从上次运行完的条数开始
                    cur_st = cur_st + curr_strip - onceNum + 1
                    addFlag = False
                    addSta = True
                    # curr_round = curr_round + 1
               else:
                    curr_round = curr_round + 1
                    cur_st = cur_st + 1

                    # 更新到数据库
                    updateSettCR(cursor, curr_round)

               # 继续运行的信息
               if(addSta) :
                   rowAll = curr_strip
               else :
                   rowAll = 0

               participle(conn, cursor, cur_st , curr_round * onceNum, curr_round,rowAll)

            else :
               flag = False

        cursor.close()  # 关闭连接
        conn.close()



        # status = participle(conn,cursor,i+1,i+onceNum,k)
    cursorF, connF = connection(database, hostname, port, user, passwd)  # 连接数据库
    # 获去目前运行到第几轮
    nowDF = getNowMess(cursorF)
    if nowDF is not None:
        dealUuid = []
        curr_round = 0
        all_round = 0
        curr_strip = 0
        for index, row in nowDF.iterrows():  # 获取每行的index、row
            curr_round = int(row['ALL_ROUND'])
            all_round = int(row['CURR_ROUND'])
            curr_strip = int(row['CURR_STRIP'])
        if (curr_round == all_round):
            # 更新词频
            updateWordFrequency(cursorF)
            # 获取dealUuid
            dealUuidDF = getdealUuid(cursorF)
            if(dealUuidDF is not None) :
                if(dealUuidDF.iat[0,0] != '') :
                   dealUuid = dealUuidDF.iat[0,0].split(",")
            # 更新到数据库
            updateOpiAnaly(cursorF,dealUuid)
            # 状态置成1
            updateSettDealFlag(cursorF)
            #删除数据库里的无用词典
            # deleteOtherWord(cursor)
    logging.info('民意分析结束...')

    if(status == "success") :
       return status
    else :
        return "Failed"


