# -*- coding: utf-8 -*-
'''
@author: liuzd
@time: 2017/8/24 09:00
@Function: top5
'''
import pandas as pd
import datetime
import sys
import fpGrowth as fp
import time
import shortuuid
import os
from commonUtil import getLog  # 从文件路径为connectionDB2的文件中导入日志
from commonUtil import getConf# 从文件路径为connectionDB2的文件中导入所有的函数
reload(sys)
sys.setdefaultencoding('utf-8')
logging = getLog()

# 所有数据
def mapK(cursor, reFlag, flag):
    se = 'select distinct ISSUE_TYPE_NAME_1 from DATAPREDICT_TOP5_INFO where QWS = ' + "'" + reFlag + "'  and PETITION_DATE_FLAG = " + "'" + flag + "' and ISSUE_TYPE_NAME_1 != ' '"
    logging.warn(" mapK  sql se: %s\t" % (se))
    cursor.execute(se)
    se = cursor.fetchall()
    listse = list(se)
    listIss = []
    for i in listse:
        Iss = "".join(list(i))  # 问题类别code
        listIss.append(Iss)
    seObj = 'SELECT DISTINCT ISSUE_TYPE_NAME_2 FROM DATAPREDICT_TOP5_INFO where  QWS = ' + "'" + reFlag + "'  and PETITION_DATE_FLAG = " + "'" + flag + "' and ISSUE_TYPE_NAME_2 !=' '"
    logging.warn(" mapK  sql seObj: %s\t" % (seObj))
    cursor.execute(seObj)
    rowObj = cursor.fetchall()
    listObj = list(rowObj)
    lis = []
    for i in listObj:
        Obj = "".join(list(i))  # 行政级别code
        arr = []
        for j in listIss:
            arra = []
            arra.append(Obj)  # 拼接行政级别code
            arra.append(j)  # 拼接问题类别code
            arr.append(arra)  # 存入集合中
        lis.append(arr)
    k = 0
    dfAll = pd.DataFrame()
    while k < len(lis):
        df = pd.concat([pd.DataFrame(lis[k], columns=['iss1', 'iss2'])], ignore_index=True)
        dfAll = dfAll.append(df)
        k += 1
    dfAll.index = range(len(dfAll))
    return dfAll


# 统计区委问题top5
def summaryQW(cursor, startTimeM, endTimeM):
    sqlQW = "select mm.ISSUE_REGION_FLAG,mm.Issue_Region_Code4,mm.Petition_Date10,mm.aoid from(select m.ISSUE_REGION_FLAG,m.Issue_Region_Code4,m.Petition_Date10,m.aoid,count(1) co from(select aa.ISSUE_REGION_FLAG,aa.Issue_Region_Code4,aa.Petition_Date10,aa.aoid from (SELECT o.ISSUE_REGION_FLAG,substr(a.Issue_Region_Code,1,4) Issue_Region_Code4,a.oid aoid,substr(a.Petition_Date,1,10) Petition_Date10 FROM PETITION_BASIC_INFO a,ORGANIZATION_SET o where substr(a.Issue_Region_Code,1,4)=substr(o.Issue_Region_Code,1,4) and a.Region_Code!='310000000000' and a.Petition_Class_Code='1' and a.Petition_Date >""'" + startTimeM + "'"" and a.Petition_Date<""'" + endTimeM + "'"" ) aa left join ISSUE_TYPE_INFO b on aa.aoid=b.Petition_Basic_Info_Oid where b.ISSUE_TYPE_CODE != ' ') m group by m.ISSUE_REGION_FLAG,m.Issue_Region_Code4,m.Petition_Date10,m.aoid)mm where mm.co >1"
    logging.info(" summaryQW  sql: %s\t" % (sqlQW))
    cursor.execute(sqlQW)
    rows = cursor.fetchall()
    if rows:
        dfQW = pd.DataFrame(rows)
        dfQW.columns = ['Issue_Region_Flag', 'Issue_Region_Code4', 'data', 'oid']
        dfQW['Issue_Region_Code4'] = dfQW.Issue_Region_Code4.replace(
            {'3119': '3115', '3103': '3101', '3108': '3106'})  # 合并区
        return dfQW
    else:
        dfQW = pd.DataFrame(rows)
        return dfQW


# 统计市问题top5
def summaryS(cursor, startTimeM, endTimeM):
    sqlQW = "select * from(select m.oid,count(1) co from(SELECT substr(a.Issue_Region_Code,1,4) Issue_Region_Code4,a.oid,a.Petition_Date FROM PETITION_BASIC_INFO a left join ISSUE_TYPE_INFO b on a.Oid=b.Petition_Basic_Info_Oid where a.Region_Code='310000000000' and a.Issue_Region_Code like '31%'  and a.Petition_Class_Code='1' and a.Petition_Date >""'" + startTimeM + "'"" and a.Petition_Date<""'" + endTimeM + "'"" ) m group by m.oid)mm where mm.co>1"
    logging.info(" summaryS  sql: %s\t" % (sqlQW))
    cursor.execute(sqlQW)
    rows = cursor.fetchall()
    dfS = pd.DataFrame(rows)
    if dfS.empty == True:
        return dfS
    else:
        dfS.columns = ['oid', 'cou']
        return dfS


# 取出所有组织机构
def selectOrganization(cursor):
    print ('开始从数据库中读组织机构数据')
    sql = "select o.org_cname Issue_Region_Name , substr(o.org_code,1,4) Issue_Region_Code4 from organization o " \
          "where o.org_code like '31%00000000' and substr(o.org_code,1,4) not in ('3119','3103','3108') " \
          "group by substr(o.org_code,1,4), o.org_cname "
    # 每次执行一次查询
    cursor.execute(sql)
    rows = cursor.fetchall()
    org_df = pd.DataFrame(rows)
    org_df.columns = ['Issue_Region_Name', 'Issue_Region_Code4']
    return org_df


# ##################     一年的时间(判断是否是任务月) 每月20号为任务月    ################## #
def yearOfMission(cur, today):
    if cur.month == 12 and cur.day > 20:
        startTime = (datetime.datetime(cur.year, cur.month, 20)).strftime("%Y-%m-%d")
        endTime = today
        return startTime, endTime
    else:
        startTime = (datetime.datetime(cur.year - 1, 12, 20)).strftime("%Y-%m-%d")
        endTime = today
        return startTime, endTime


# ################## 半年的时间 去年12月20号到今年6月20号 今年6月20号到12月20号 ################## #
def sixMonthsOfMission(cur, today):
    if cur.month >= 6 and cur.month <= 11:
        if cur.month == 6 and cur.day <= 20:
            startTime = (datetime.datetime(cur.year - 1, 12, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime
        else:
            startTime = (datetime.datetime(cur.year, 6, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime
    if cur.month == 12 or (cur.month >= 1 and cur.month <= 5):
        if cur.month == 12 and cur.day <= 20:
            startTime = (datetime.datetime(cur.year, 6, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime
        else:
            startTime = (datetime.datetime(cur.year, 12, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime


# # 季度的时间 去年12月20号到今年2月20号 今年3月20号到5月20号 今年6月20号到8月20号 今年9月20号到11月20号# #
def quarterlyOfMission(cur, today):
    if cur.month == 12 or cur.month == 1 or cur.month == 2:
        if cur.month == 12 and cur.day <= 20:
            startTime = (datetime.datetime(cur.year, 9, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime
        if cur.month == 12 and cur.day > 20:
            startTime = (datetime.datetime(cur.year, 12, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime
        else:
            startTime = (datetime.datetime(cur.year - 1, 12, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime
    if cur.month >= 9 and cur.month < 12:
        if cur.month == 9 and cur.day < 20:
            startTime = (datetime.datetime(cur.year, 6, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime
        else:
            startTime = (datetime.datetime(cur.year, 9, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime
    if cur.month >= 6 and cur.month < 9:
        if cur.month == 6 and cur.day < 20:
            startTime = (datetime.datetime(cur.year, 3, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime
        else:
            startTime = (datetime.datetime(cur.year, 6, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime
    if cur.month >= 3 and cur.month < 6:
        if cur.month == 3 and cur.day < 20:
            startTime = (datetime.datetime(cur.year - 1, 12, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime
        else:
            startTime = (datetime.datetime(cur.year, 3, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime


# ##################                 一个月的时间               ################## #
def monthOfMission(cur, today):
    if cur.month == 1:
        if cur.day > 20:
            startTime = (datetime.datetime(cur.year, cur.month, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime
        else:
            startTime = (datetime.datetime(cur.year - 1, 12, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime
    else:
        if cur.day > 20:
            startTime = (datetime.datetime(cur.year, cur.month, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime
        else:
            startTime = (datetime.datetime(cur.year, cur.month - 1, 20)).strftime("%Y-%m-%d")
            endTime = today
            return startTime, endTime


def mainDeal(df, cursor, conn, today, flag, regionFlag):
    lisAll = []
    for index, row in df.iterrows():
        try:
            sqlSelect = 'select ISSUE_TYPE_NAME from ISSUE_TYPE_INFO where PETITION_BASIC_INFO_OID = ' + "'" + \
                        row[
                            'oid'] + "'"
            cursor.execute(sqlSelect)
            rows = cursor.fetchall()
            pro = pd.DataFrame(rows)
            lis = []
            for i in pro.iloc[:, 0]:
                lis.append(i)
                lisAll.append(lis)
        except Exception as e:
            logging.error(" mainDeal() Exception: %s\t" % (e))
    resultCity = []
    for itemset, support in fp.find_frequent_itemsets(lisAll, 0, True):
        resultCity.append((itemset, support))

    df = mapK(cursor, regionFlag, flag)  # key 行政级别 value 问题类别list  用来插补数据 df
    path = os.path.realpath(sys.argv[0])
    createName = getConf("common", "createName", path)
    createId = getConf("common", "createId", path)
    for itemset, support in resultCity:
        # 判断list长度，过滤不符合条件的集合
        if (len(itemset) == 2):  # 统计问题关联关系
            name1 = 'select ISSUE_TYPE_CODE from PETITION_ISSUE_INFO  WHERE ISSUE_TYPE_NAME = ' + "'" + \
                    itemset[0] + "'"
            cursor.execute(name1)
            name1 = cursor.fetchall()
            name2 = 'select ISSUE_TYPE_CODE from PETITION_ISSUE_INFO  WHERE ISSUE_TYPE_NAME = ' + "'" + \
                    itemset[1] + "'"
            cursor.execute(name2)
            name2 = cursor.fetchall()
            df = df[(df['iss1'] != itemset[0]) | (df['iss2'] != itemset[1])]
            try:
                sqlInsert = "insert into DATAPREDICT_TOP5_ALL_DATA_INFO(OID, ISSUE_TYPE_CODE_1, ISSUE_TYPE_NAME_1,ISSUE_TYPE_CODE_2,ISSUE_TYPE_NAME_2, SUPPORT,qws, PETITION_DATE_FLAG,CREATE_DATE,CREATOR_NAME,CREATOR_ID,MODIFY_DATE,MODIFY_NAME,MODIFY_ID) VALUES ( '%s', '%s', '%s',  '%s', '%s', %s,   '%s', '%s',  date('%s'), '%s', '%s', date('%s'), '%s', '%s')" % (
                    str(shortuuid.ShortUUID().random(length=20)), name1[0][0], str(itemset[0]),
                    name2[0][0], str(itemset[1]), support, regionFlag, flag, str(today), createName, createId, str(today), '', '')
                logging.info(" itemset 2 insert into  sql: %s\t" % (sqlInsert))
                cursor.execute(sqlInsert)
            except Exception as e:
                logging.error(" itemset 2 sql error : %s\t" % (e))
                conn.rollback()  # 回滚

        if (len(itemset) == 1):  # 统计问题top5 个数
            name1 = 'select ISSUE_TYPE_CODE from PETITION_ISSUE_INFO  WHERE ISSUE_TYPE_NAME = ' + "'" + \
                    itemset[0] + "'"
            cursor.execute(name1)
            name1 = cursor.fetchall()
            try:
                sqlInsert = "insert into DATAPREDICT_TOP5_COUNT_INFO(OID, ISSUE_TYPE_CODE, ISSUE_TYPE_NAME, SUPPORT,qws, PETITION_DATE_FLAG,CREATE_DATE,CREATOR_NAME,CREATOR_ID,MODIFY_DATE,MODIFY_NAME,MODIFY_ID) VALUES ( '%s', '%s', '%s',  %s,  '%s', '%s',  date('%s'), '%s', '%s', date('%s'), '%s', '%s')" % (
                    str(shortuuid.ShortUUID().random(length=20)), name1[0][0], str(itemset[0]),
                     support, regionFlag, flag, str(today), createName, createId, str(today), '', '')
                logging.info("insert DATAPREDICT_TOP5_COUNT_INFO  sql  : %s\t" % (sqlInsert))
                cursor.execute(sqlInsert)
            except Exception as e:
                logging.error(" itemset 1 sql error : %s\t" % (e))
                conn.rollback()  # 回滚
    for index, row in df.iterrows():
        name1 = 'select ISSUE_TYPE_CODE from PETITION_ISSUE_INFO  WHERE ISSUE_TYPE_NAME = ' + "'" + \
                row['iss1'] + "'"
        cursor.execute(name1)
        name1 = cursor.fetchall()
        name2 = 'select ISSUE_TYPE_CODE from PETITION_ISSUE_INFO  WHERE ISSUE_TYPE_NAME = ' + "'" + \
                row['iss2'] + "'"
        cursor.execute(name2)
        name2 = cursor.fetchall()
        try:
            sqlInsert = "insert into DATAPREDICT_TOP5_ALL_DATA_INFO(OID, ISSUE_TYPE_CODE_1, ISSUE_TYPE_NAME_1,ISSUE_TYPE_CODE_2,ISSUE_TYPE_NAME_2, SUPPORT,qws, PETITION_DATE_FLAG,CREATE_DATE,CREATOR_NAME,CREATOR_ID,MODIFY_DATE,MODIFY_NAME,MODIFY_ID) VALUES ( '%s', '%s', '%s',  '%s', '%s', %s,  '%s', '%s',  date('%s'), '%s', '%s', date('%s'), '%s', '%s')" % (
                str(shortuuid.ShortUUID().random(length=20)), name1[0][0], row['iss1'],
                name2[0][0], row['iss2'], 0, regionFlag, flag, str(today), createName, createId, str(today), '', '')
            logging.info("insert DATAPREDICT_TOP5_ALL_DATA_INFO 2  sql  : %s\t" % (sqlInsert))
            cursor.execute(sqlInsert)
        except Exception as e:
            logging.error(" itemset 2 buquan sql error : %s\t" % (e))
            conn.rollback()  # 回滚

def mainTop5C(cursor, conn, modelCycle):
    try:
        start = time.clock()
        today = datetime.datetime.now().strftime('%Y-%m-%d')  # 当前日期
        cur = datetime.datetime.now()
        startTimeM, endTimeM = monthOfMission(cur, today)  # 一个月的时间
        startTimeQ, endTimeQ = quarterlyOfMission(cur, today)  # 季度的时间
        # startTimeS, endTimeS = sixMonthsOfMission(cur, today)  # 半年的时间
        startTimeY, endTimeY = yearOfMission(cur, today)  # 一年的时间
        dfQWM = summaryQW(cursor, startTimeM, endTimeM)  # 统计区委问题top5  一个月的dataframe
        dfQWQ = summaryQW(cursor, startTimeQ, endTimeQ)  # 一个季度的dataframe
        dfQWY = summaryQW(cursor, startTimeY, endTimeY)

        dfSM = summaryS(cursor, startTimeM, endTimeM)  # 统计上海市问题top5  一个月的dataframe
        dfSQ = summaryS(cursor, startTimeQ, endTimeQ)  # 一个季度的dataframe
        dfSY = summaryS(cursor, startTimeY, endTimeY)

        org_df = selectOrganization(cursor)  # 取出所有组织机构
        # 判断df是否为空，即根据时间取的数据是否为空（月，季度，半年，年）
        if dfQWM.empty != True:
            logging.info("dfQWM")
            petition_dfM = pd.merge(org_df, dfQWM, on=['Issue_Region_Code4'])  # 合并 查区域名
            mainDeal(petition_dfM, cursor, conn, today, 'm', 'qw')
        if dfQWQ.empty != True:
            logging.info("dfQWQ")
            petition_dfQ = pd.merge(org_df, dfQWQ, on=['Issue_Region_Code4'])  # 合并 查区域名
            mainDeal(petition_dfQ, cursor, conn, today, 'q', 'qw')
        if dfQWY.empty != True:
            logging.info("dfQWY")
            petition_dfY = pd.merge(org_df, dfQWY, on=['Issue_Region_Code4'])  # 合并 查区域名
            mainDeal(petition_dfY, cursor, conn, today, 'y', 'qw')

        if dfSM.empty != True:
            logging.info("dfSM")
            # petition_dfM = pd.merge(org_df, dfSM, on=['Issue_Region_Code4'])  # 合并 查区域名
            mainDeal(dfSM, cursor, conn, today, 'm', 's')
        if dfSQ.empty != True:
            logging.info("dfSQ")
            # petition_dfQ = pd.merge(org_df, dfSQ, on=['Issue_Region_Code4'])  # 合并 查区域名
            mainDeal(dfSQ, cursor, conn, today, 'q', 's')
        if dfSY.empty != True:
            logging.info("dfSY")
            # petition_dfY = pd.merge(org_df, dfSY, on=['Issue_Region_Code4'])  # 合并 查区域名
            mainDeal(dfSY, cursor, conn, today, 'y', 's')
        # 该语句将清除表中所有数据，但由于这一操作会记日志，因此执行速度会相对慢一些，另外要注意的是，
        # 如果表较大，为保证删除操作的成功，应考虑是否留有足够大的日志空间
        yesTime = datetime.datetime.now() + datetime.timedelta(days=-modelCycle)
        yesTimeNyr = yesTime.strftime('%Y-%m-%d')  # 格式化输出 前n天
        sqlDelete1 = "delete from DATAPREDICT_TOP5_COUNT_INFO where substr(CREATE_DATE,1,10)<='" + yesTimeNyr + "'"  # 删表数据
        sqlDelete2 = "delete from DATAPREDICT_TOP5_ALL_DATA_INFO where substr(CREATE_DATE,1,10)<='" + yesTimeNyr + "'"
        logging.info(" delete table 1  sql: %s\t" % (sqlDelete1))
        logging.info(" delete table  2 sql: %s\t" % (sqlDelete2))
        cursor.execute(sqlDelete1)  # 执行
        cursor.execute(sqlDelete2)  # 执行
        end = time.clock()  # 运行结束时间
        print(u'\n存储完毕，用时：%0.2f秒' % (end - start))  # 打印运行总时间
    except Exception as e:
        logging.error("top5c Exception: %s\t" % (e))  # 打印异常
    finally:
        # cursor.close()  # 关闭连接
        pass
