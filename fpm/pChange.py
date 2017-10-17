# -*- coding: utf-8 -*-
'''
@author: liuzd
@time: 2017/8/24 09:00
@Function: 问题类别变化情况
'''
import pandas as pd
import datetime
import shortuuid
import sys
import time
import os
from commonUtil import getLog  # 从文件路径为connectionDB2的文件中导入日志
from commonUtil import getConf  # 从文件路径为connectionDB2的文件中导入所有的函数

reload(sys)
sys.setdefaultencoding('utf-8')
# 获取日志对象
logging = getLog()

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
            startTime = (datetime.datetime(cur.year - 1, 12, 20)).strftime("%Y-%m-%d")
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


# 统计区委自办转办数量
def summaryQW(cursor, startTime, endTime):
    sqlQW = "select b1.ISSUE_REGION_FLAG,b1.Issue_Region_Code4,b1.num1,b2.num2 from (select m.ISSUE_REGION_FLAG,m.Issue_Region_Code4,count(1) num1 from ( SELECT o.ISSUE_REGION_FLAG,substr(a.Issue_Region_Code,1,4) Issue_Region_Code4 FROM PETITION_BASIC_INFO a,ORGANIZATION_SET o where substr(a.Issue_Region_Code,1,4)=substr(o.Issue_Region_Code,1,4) and a.Region_Code!='310000000000' and a.Petition_Class_Code='1' and substr(a.Petition_Source_Code,1,2)='01' and a.Petition_Date >""'" + startTime + "'"" and a.Petition_Date<""'" + endTime + "'"") m group by m.ISSUE_REGION_FLAG,m.Issue_Region_Code4 ) b1,(select m.ISSUE_REGION_FLAG,m.Issue_Region_Code4,count(1) num2 from ( SELECT o.ISSUE_REGION_FLAG,substr(a.Issue_Region_Code,1,4) Issue_Region_Code4 FROM PETITION_BASIC_INFO a,ORGANIZATION_SET o where substr(a.Issue_Region_Code,1,4)=substr(o.Issue_Region_Code,1,4) and a.Region_Code!='310000000000' and a.Petition_Class_Code='1' and substr(a.Petition_Source_Code,1,2)!='01' and a.Petition_Date >""'" + startTime + "'"" and a.Petition_Date<""'" + endTime + "'"" ) m group by m.ISSUE_REGION_FLAG,m.Issue_Region_Code4 ) b2 where b1.Issue_Region_Code4=b2.Issue_Region_Code4 and b1.ISSUE_REGION_FLAG=b2.ISSUE_REGION_FLAG"
    logging.info("summaryQW  sql: %s\t" % (sqlQW))
    cursor.execute(sqlQW)
    rows = cursor.fetchall()
    dfQW = pd.DataFrame(rows)
    if dfQW.empty == True:
        return dfQW
    else:
        dfQW.columns = ['Issue_Region_Flag', 'Issue_Region_Code4', 'self', 'turn']
        dfQW['Issue_Region_Code4'] = dfQW.Issue_Region_Code4.replace(
            {'3119': '3115', '3103': '3101', '3108': '3106'})  # 合并区
        dfQW = dfQW.groupby(['Issue_Region_Flag', 'Issue_Region_Code4'], as_index=False)['self', 'turn'].sum()
        return dfQW


# 统计市自办转办数量
def summaryS(cursor, startTime, endTime):
    sqlS = "select b1.ISSUE_REGION_FLAG,b1.num1,b2.num2 from (select m.ISSUE_REGION_FLAG,count(1) num1 from ( SELECT o.ISSUE_REGION_FLAG,substr(a.Issue_Region_Code,1,4) Issue_Region_Code4 FROM PETITION_BASIC_INFO a,ORGANIZATION_SET o where substr(a.Region_Code,1,4)=substr(o.Issue_Region_Code,1,4) and a.Region_Code='310000000000' and a.Petition_Class_Code='1'  and substr(a.Petition_Source_Code,1,2)='01') m group by m.ISSUE_REGION_FLAG ) b1,(select m.ISSUE_REGION_FLAG,count(1) num2 from (SELECT o.ISSUE_REGION_FLAG,substr(a.Issue_Region_Code,1,4) Issue_Region_Code4 FROM PETITION_BASIC_INFO a,ORGANIZATION_SET o where substr(a.Region_Code,1,4)=substr(o.Issue_Region_Code,1,4) and a.Region_Code='310000000000' and a.Petition_Class_Code='1'  and substr(a.Petition_Source_Code,1,2)!='01' and a.Petition_Date >""'" + startTime + "'"" and a.Petition_Date<""'" + endTime + "'"" ) m group by m.ISSUE_REGION_FLAG) b2 where  b1.ISSUE_REGION_FLAG=b2.ISSUE_REGION_FLAG "
    logging.info("summaryS  sql: %s\t" % (sqlS))
    cursor.execute(sqlS)
    rows = cursor.fetchall()
    dfS = pd.DataFrame(rows)
    if dfS.empty == True:
        return dfS
    else:
        dfS.columns = ['Issue_Region_Flag', 'self', 'turn']
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


# 查找区
def findQuData(qu_data, cursor, today, conn, flag, createName, createId):
    qu = qu_data[qu_data['Issue_Region_Flag'] == 2]  # 区
    for index, row in qu.iterrows():
        try:
            sqlInsert = "insert into DATAPREDICT_QC_INFO(OID, SELF, TURN, Issue_Region_Name,Issue_Region_Code4, Issue_Region_Flag, PETITION_DATE_FLAG, CREATE_DATE, CREATOR_NAME, CREATOR_ID, MODIFY_DATE, MODIFY_NAME, MODIFY_ID) VALUES ('%s',%s,%s,'%s','%s','%s','%s', date('%s'), '%s', '%s', date('%s'), '%s', '%s')" % (
                str(shortuuid.ShortUUID().random(length=20)), row["self"], row["turn"], row["Issue_Region_Name"],
                row["Issue_Region_Code4"], row["Issue_Region_Flag"], flag, today, createName, createId, str(today), '',
                '')
            logging.info("findQuData() insert sql: %s\t" % (sqlInsert))
            cursor.execute(sqlInsert)
        except Exception as e:
            logging.error("findQuData() insert error: %s\t" % (e))
            conn.rollback()  # 回滚


# 查找派驻
def findWeiData(qu_data, cursor, today, conn, flag, createName, createId):
    paizhu = qu_data[qu_data['Issue_Region_Flag'] == 3]  # 派驻
    for index, row in paizhu.iterrows():
        try:
            sqlInsert = "insert into DATAPREDICT_QC_INFO(OID, SELF, TURN, Issue_Region_Name,Issue_Region_Code4, Issue_Region_Flag, PETITION_DATE_FLAG, CREATE_DATE, CREATOR_NAME, CREATOR_ID, MODIFY_DATE, MODIFY_NAME, MODIFY_ID) VALUES ('%s',%s,%s,'%s','%s','%s','%s', date('%s'), '%s', '%s', date('%s'), '%s', '%s')" % (
                str(shortuuid.ShortUUID().random(length=20)), row["self"], row["turn"], row["Issue_Region_Name"],
                row["Issue_Region_Code4"], row["Issue_Region_Flag"], flag, today, createName, createId, str(today), '',
                '')
            logging.info("findWeiData() insert sql: %s\t" % (sqlInsert))
            cursor.execute(sqlInsert)
        except Exception as e:
            logging.error("findWeiData() insert error: %s\t" % (e))  # 打印异常
            conn.rollback()  # 回滚


# 查找市
def findCityData(petition_df, cursor, today, conn, flag, createName, createId):
    for index, row in petition_df.iterrows():
        try:
            sqlInsert = "insert into DATAPREDICT_QC_INFO(OID, SELF,TURN, Issue_Region_Name,Issue_Region_Code4,Issue_Region_Flag, PETITION_DATE_FLAG, CREATE_DATE, CREATOR_NAME, CREATOR_ID, MODIFY_DATE, MODIFY_NAME, MODIFY_ID) VALUES ('%s',%s,%s,'%s','%s', '%s',  '%s', date('%s'), '%s', '%s', date('%s'), '%s', '%s')" % (
                str(shortuuid.ShortUUID().random(length=20)), row["self"], row["turn"], '上海市', '',
                row["Issue_Region_Flag"], flag, today, createName, createId, str(today), '', '')
            logging.info("findCityData() insert sql: %s\t" % (sqlInsert))
            cursor.execute(sqlInsert)
        except Exception as e:
            logging.error("findCityData() insert error: %s\t" % (e))  # 打印异常
            conn.rollback()  # 回滚


def mainQC(cursor, conn, modelCycle):
    try:
        start = time.clock()
        path = os.path.realpath(sys.argv[0])
        createName = getConf("common", "createName", path)  # 获取创建人
        createId = getConf("common", "createId", path)  # 获取创建人ID
        today = datetime.datetime.now().strftime('%Y-%m-%d')  # 当前日期
        cur = datetime.datetime.now()
        startTimeM, endTimeM = monthOfMission(cur, today)  # 一个月的时间
        startTimeQ, endTimeQ = quarterlyOfMission(cur, today)  # 季度的时间
        # startTimeS, endTimeS = sixMonthsOfMission(cur, today)  # 半年的时间
        startTimeY, endTimeY = yearOfMission(cur, today)  # 一年的时间

        logging.info("startTimeM  time  : %s\t" % (startTimeM))
        logging.info("endTimeM  time  : %s\t" % (endTimeM))
        logging.info("startTimeQ  time  : %s\t" % (startTimeQ))
        logging.info("endTimeQ  time  : %s\t" % (endTimeQ))
        logging.info("startTimeY  time  : %s\t" % (startTimeY))
        logging.info("endTimeY  time  : %s\t" % (endTimeY))

        dfQWY = summaryQW(cursor, startTimeY, endTimeY)  # 一年数据
        dfQQ = summaryQW(cursor, startTimeQ, endTimeQ)  # 季度数据
        dfMM = summaryQW(cursor, startTimeM, endTimeM)  # 一个月数据
        org_df = selectOrganization(cursor)  # 取出所有组织机构

        if dfQWY.empty != True:
            petition_df = pd.merge(org_df, dfQWY, on=['Issue_Region_Code4'])  # 合并 查区域名字
            findQuData(petition_df, cursor, today, conn, "y", createName, createId)  # 查找区一年数据
            findWeiData(petition_df, cursor, today, conn, "y", createName, createId)  # 查找派驻一年数据
        if dfQQ.empty != True:
            petition_df = pd.merge(org_df, dfQQ, on=['Issue_Region_Code4'])  # 合并 查区域名字
            findQuData(petition_df, cursor, today, conn, "q", createName, createId)  # 查找区季度数据
            findWeiData(petition_df, cursor, today, conn, "q", createName, createId)  # 查找派驻季度数据
        if dfMM.empty != True:
            petition_df = pd.merge(org_df, dfMM, on=['Issue_Region_Code4'])  # 合并 查区域名字
            findQuData(petition_df, cursor, today, conn, "m", createName, createId)  # 查找区一个月数据
            findWeiData(petition_df, cursor, today, conn, "m", createName, createId)  # 查找派驻一个月数据

        # dfS = summaryS(cursor)
        dfSY = summaryS(cursor, startTimeY, endTimeY)  # 一年数据
        dfSQ = summaryS(cursor, startTimeQ, endTimeQ)  # 季度数据
        dfSM = summaryS(cursor, startTimeM, endTimeM)  # 一个月数据

        if dfSY.empty != True:
            findCityData(dfSY, cursor, today, conn, 'y', createName, createId)
        if dfSQ.empty != True:
            findCityData(dfSQ, cursor, today, conn, 'q', createName, createId)
        if dfSM.empty != True:
            findCityData(dfSM, cursor, today, conn, 'm', createName, createId)

        # 该语句将清除表中所有数据，但由于这一操作会记日志，因此执行速度会相对慢一些，另外要注意的是，
        # 如果表较大，为保证删除操作的成功，应考虑是否留有足够大的日志空间
        yesTime = datetime.datetime.now() + datetime.timedelta(days=-modelCycle)
        yesTimeNyr = yesTime.strftime('%Y-%m-%d')  # 格式化输出 前n天
        sqlDelete = "delete from DATAPREDICT_QC_INFO where substr(CREATE_DATE,1,10)<='" + yesTimeNyr + "'"  # 删表数据
        logging.info(" delete table  sql: %s\t" % (sqlDelete))
        cursor.execute(sqlDelete)  # 执行
        end = time.clock()  # 运行结束时间
        logging.info(" 存储完毕:，用时： %s\t" % (end - start))
    except Exception as e:
        logging.error("Exception: %s\t" % (e)) # 打印异常
    finally:
        # cursor.close()  # 关闭连接
        pass
