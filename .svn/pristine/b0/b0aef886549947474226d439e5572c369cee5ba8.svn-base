# -*- coding: utf-8 -*-
'''
@author: liuzd
@time: 2017/8/15 14:58
@Function: 问题类别与行政级别关联分析并分区
'''

import pandas as pd
import fpGrowth as fp
import datetime
import shortuuid
import sys
import time
import os
from commonUtil import getLog  # 从文件路径为connectionDB2的文件中导入日志
from commonUtil import getConf# 从文件路径为connectionDB2的文件中导入所有的函数
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


# 通过code查找name,并插入数据库，并且插入数据库
def codeFindName(cursor, itemset, today, conn, support, area, flag):
    # 根据问题类别code,行政级别code查找信息
    # print u'根据问题类别code,行政级别code查找信息'
    info = 'select a.ISSUE_TYPE_NAME,b.OBJECT_CLASS_NAME,b.ISSUE_REGION_CODE from PETITION_ISSUE_INFO a,PETITION_BASIC_INFO b WHERE a.Region_Code = b.Region_Code and a.PETITION_NO = b.PETITION_NO and a.ISSUE_TYPE_CODE = ' + "'" + \
           itemset[0] + "' and b.OBJECT_CLASS_CODE =" + "'" + itemset[1] + "'"
    cursor.execute(info)
    info = cursor.fetchall()
    path = os.path.realpath(sys.argv[0])
    createName = getConf("common", "createName", path)
    createId = getConf("common", "createId", path)
    try:
        sqlInsert = "insert into DATAPREDICT_FPM_QL_ALL_DATA_INFO(OID, ISSUE_TYPE_CODE, ISSUE_TYPE_NAME, OBJECT_CLASS_CODE, OBJECT_CLASS_NAME, SUPPORT, REGIN_NAME, REGIN_CODE, PETITION_DATE_FLAG,CREATE_DATE,CREATOR_NAME,CREATOR_ID,MODIFY_DATE,MODIFY_NAME,MODIFY_ID) VALUES ( '%s', '%s', '%s', '%s', '%s', %s,'%s','%s','%s', date('%s'), '%s', '%s', date('%s'), '%s', '%s')" % (
            str(shortuuid.ShortUUID().random(length=20)), str(itemset[0]), info[0][0], itemset[1], info[0][1],
            support, area, info[0][2], flag, today, createName, createId, today, '', '')
        logging.info("codeFindName  insert sql  : %s\t" % (sqlInsert))
        cursor.execute(sqlInsert)
    except Exception as e:
        logging.error("insert DATAPREDICT_FPM_QL_ALL_DATA_INFO Failed : %s\t" % (e))
        conn.rollback()  # 回滚


# 取问题类别code和处理方式code,并且组合成多个list
def dealData(df):
    wCategory = df['Issue_Type_Code']
    bWay = df['Object_Class_Code']
    endData = zip(wCategory, bWay)
    listD = []
    # 元组转换list
    for j in endData:
        listD.append(list(j))
    return listD


# key 行政级别 value 问题类别list  用来插补数据
def mapK(cursor, name, flag):
    se = 'select distinct ISSUE_TYPE_CODE from DATAPREDICT_FPM_QL_INFO where REGIN_NAME = ' + "'" + name + "'  and PETITION_DATE_FLAG = " + "'" + flag + "' "
    logging.info("find  distinct ISSUE_TYPE_CODE sum : %s\t" % (se))
    cursor.execute(se)
    se = cursor.fetchall()
    listse = list(se)
    listIss = []
    for i in listse:
        Iss = "".join(list(i))  # 问题类别code
        listIss.append(Iss)
    seObj = 'SELECT DISTINCT OBJECT_CLASS_CODE FROM DATAPREDICT_FPM_QL_INFO where REGIN_NAME = ' + "'" + name + "' and PETITION_DATE_FLAG = " + "'" + flag + "' "
    cursor.execute(seObj)
    rowObj = cursor.fetchall()
    listObj = list(rowObj)
    lis = []
    for i in listObj:
        Obj = "".join(list(i))  # 行政级别code
        arr = []
        for k in listIss:
            arra = []
            arra.append(Obj)  # 拼接行政级别code
            arra.append(k)  # 拼接问题类别code
            arr.append(arra)  # 存入集合中
        lis.append(arr)
    k = 0
    dfAll = pd.DataFrame()
    while k < len(lis):
        df = pd.concat([pd.DataFrame(lis[k], columns=['OBJECT_CLASS_CODE', 'ISSUE_TYPE_CODE'])], ignore_index=True)
        dfAll = dfAll.append(df)
        k += 1
    dfAll.index = range(len(dfAll))
    # dfAll.to_csv('C:\\Users\\Administrator\\Desktop\\aaa.csv')
    return dfAll


# 上海市模型数据 (一年，半年，季度，一个月)
def findCityData(petition_df, cursor, today, conn, sup, flag):
    scity = petition_df[petition_df.qws == 1]  # 市
    listD = dealData(scity)
    resultCity = []
    # 调用fpgrowth算法
    for itemset, support in fp.find_frequent_itemsets(listD, sup, True):
        resultCity.append((itemset, support))
    shi = u'上海市'
    df = mapK(cursor, shi, flag)  # key 行政级别 value 问题类别list  用来插补数据 df
    dfchabu = pd.DataFrame()
    # 循环算法输出结果存入数据库
    for itemset, support in resultCity:
        # 判断list长度，过滤不符合条件的集合
        if (len(itemset) > 1):
            # itemset[0] 问题类别code
            # itemset[1] 行政级别code
            df = df[(df['ISSUE_TYPE_CODE'] != itemset[0]) | (df['OBJECT_CLASS_CODE'] != itemset[1])]
            dfchabu = df
            # 比较dataframe是否含有相同的值，有则删除这行数据 返回一个新的dataframe
            # dfNew = df.drop(int(str(df[(df['ISSUE_TYPE_CODE'] == itemset[0]) & (df['OBJECT_CLASS_CODE'] == itemset[1])].iat[0, 0])))


            # 通过code查找name, 并插入数据库
            codeFindName(cursor, itemset, today, conn, support, u'上海市', flag)
    logging.info(" Number of data to be interpolated: %s\t" % (len(dfchabu)))
    # dfchabu.index = range(len(dfchabu)) # 修改序号
    path = os.path.realpath(sys.argv[0])
    createName = getConf("common", "createName", path)
    createId = getConf("common", "createId", path)
    for index, row in dfchabu.iterrows():
        infoIss = 'select ISSUE_TYPE_NAME from DATAPREDICT_FPM_QL_INFO  WHERE ISSUE_TYPE_CODE = ' + "'" + row[
            'ISSUE_TYPE_CODE'] + "'  and PETITION_DATE_FLAG = " + "'" + flag + "'"
        infoObj = 'select OBJECT_CLASS_NAME from DATAPREDICT_FPM_QL_INFO  WHERE OBJECT_CLASS_CODE = ' + "'" + row[
            'OBJECT_CLASS_CODE'] + "' and PETITION_DATE_FLAG = " + "'" + flag + "'"
        logging.info(" find data infoIss: %s\t" % (infoIss))
        logging.info(" find data infoObj: %s\t" % (infoObj))
        cursor.execute(infoIss)
        infoIss = cursor.fetchall()
        cursor.execute(infoObj)
        infoObj = cursor.fetchall()
        try:
            sqlInsert = "insert into DATAPREDICT_FPM_QL_ALL_DATA_INFO(OID, ISSUE_TYPE_CODE, ISSUE_TYPE_NAME, OBJECT_CLASS_CODE, OBJECT_CLASS_NAME, SUPPORT, REGIN_NAME, REGIN_CODE, PETITION_DATE_FLAG,CREATE_DATE,CREATOR_NAME,CREATOR_ID,MODIFY_DATE,MODIFY_NAME,MODIFY_ID) VALUES ( '%s', '%s', '%s', '%s', '%s', %s,'%s','%s','%s', date('%s'), '%s', '%s', date('%s'), '%s', '%s')" % (
                str(shortuuid.ShortUUID().random(length=20)), row['ISSUE_TYPE_CODE'], infoIss[0][0],
                row['OBJECT_CLASS_CODE'], infoObj[0][0],
                0, u'上海市', '', flag, today, createName, createId, today, '', '')
            logging.info(" s insert DATAPREDICT_FPM_QL_ALL_DATA_INFO sql: %s\t" % (sqlInsert))
            cursor.execute(sqlInsert)
        except Exception as e:
            logging.error("s insert DATAPREDICT_FPM_QL_ALL_DATA_INFO error : %s\t" % (e))
            conn.rollback()  # 回滚


# 查找区
def findQuData(qu_data, cursor, today, conn, sup, flag):
    qu = qu_data[qu_data.qws == 2]  # 区
    result = []
    for i in list(set(qu['Issue_Region_Name'])):
        # quName = org_df[org_df.Issue_Region_Code4 == i].iloc[0, 0]
        dfQu = qu_data[qu_data.Issue_Region_Name == i]
        listD = dealData(dfQu)
        # 调用fpgrowth算法
        # print u'调用fp-growth算法'
        for itemset, support in fp.find_frequent_itemsets(listD, sup, True):
            result.append((itemset, support, i))
    dic = {}
    for i in list(set(qu['Issue_Region_Name'])):
        df = mapK(cursor, i, flag)  # key 行政级别 value 问题类别list  用来插补数据 df
        dic[i] = df
    # 循环算法返回结果
    dicA = {}
    for itemset, support, area in result:
        # 判断list长度，过滤不符合条件的集合
        if (len(itemset) > 1):
            # itemset[0] 问题类别code
            # itemset[1] 行政级别code

            dic[area] = dic[area][
                (dic[area]['ISSUE_TYPE_CODE'] != itemset[0]) | (dic[area]['OBJECT_CLASS_CODE'] != itemset[1])]
            dicA[area] = dic[area]  # key 区名 value 区对应的需要插补的数据
            codeFindName(cursor, itemset, today, conn, support, area, flag)
    path = os.path.realpath(sys.argv[0])
    createName = getConf("common", "createName", path)
    createId = getConf("common", "createId", path)
    for i in list(set(qu['Issue_Region_Name'])):
        for k, v in dicA.items():
            if i == k:
                # dfchabu.index = range(len(dfchabu)) # 修改序号
                for index, row in v.iterrows():
                    infoIss = 'select ISSUE_TYPE_NAME from DATAPREDICT_FPM_QL_INFO  WHERE ISSUE_TYPE_CODE = ' + "'" + \
                              row['ISSUE_TYPE_CODE'] + "' and PETITION_DATE_FLAG = " + "'" + flag + "'"
                    logging.info(" q select DATAPREDICT_FPM_QL_INFO infoIss sql: %s\t" % (infoIss))
                    cursor.execute(infoIss)
                    infoIss = cursor.fetchall()
                    infoObj = 'select OBJECT_CLASS_NAME from DATAPREDICT_FPM_QL_INFO  WHERE OBJECT_CLASS_CODE = ' + "'" + \
                              row['OBJECT_CLASS_CODE'] + "' and PETITION_DATE_FLAG = " + "'" + flag + "'"
                    logging.info(" q select DATAPREDICT_FPM_QL_INFO infoObj sql: %s\t" % (infoObj))
                    cursor.execute(infoObj)
                    infoObj = cursor.fetchall()
                    try:
                        sqlInsert = "insert into DATAPREDICT_FPM_QL_ALL_DATA_INFO(OID, ISSUE_TYPE_CODE, ISSUE_TYPE_NAME, OBJECT_CLASS_CODE, OBJECT_CLASS_NAME, SUPPORT, REGIN_NAME, REGIN_CODE, PETITION_DATE_FLAG,CREATE_DATE,CREATOR_NAME,CREATOR_ID,MODIFY_DATE,MODIFY_NAME,MODIFY_ID) VALUES ( '%s', '%s', '%s', '%s', '%s', %s,'%s','%s','%s', date('%s'), '%s', '%s', date('%s'), '%s', '%s')" % (
                            str(shortuuid.ShortUUID().random(length=20)), row['ISSUE_TYPE_CODE'], infoIss[0][0],
                            row['OBJECT_CLASS_CODE'], infoObj[0][0],
                            0, i, '', flag, today, createName, createId, today, '', '')
                        logging.info(" q insert DATAPREDICT_FPM_QL_ALL_DATA_INFO sql: %s\t" % (sqlInsert))
                        cursor.execute(sqlInsert)
                    except Exception as e:
                        logging.error("q insert DATAPREDICT_FPM_QL_ALL_DATA_INFO error : %s\t" % (e))
                        conn.rollback()  # 回滚


# 查找委办
def findWeiData(qu_data, cursor, today, conn, sup, flag):
    resultWei = []
    wei = qu_data[qu_data.qws == 3]  # 派驻
    for i in list(set(wei['Issue_Region_Name'])):
        # weiName = org_df[org_df.Issue_Region_Code4 == i].iloc[0, 0] # 取区域名
        dfWei = qu_data[qu_data.Issue_Region_Name == i]
        listD = dealData(dfWei)
        # 调用fpgrowth算法
        # print u'调用fp-growth算法'
        for itemset, support in fp.find_frequent_itemsets(listD, sup, True):
            resultWei.append((itemset, support, i))
            # 循环算法返回结果
    dic = {}
    for i in list(set(wei['Issue_Region_Name'])):
        df = mapK(cursor, i, flag)  # key 行政级别 value 问题类别list  用来插补数据 df
        dic[i] = df

    dicA = {}
    for itemset, support, area in resultWei:
        # 判断list长度，过滤不符合条件的集合
        if (len(itemset) > 1):
            dic[area] = dic[area][
                (dic[area]['ISSUE_TYPE_CODE'] != itemset[0]) | (dic[area]['OBJECT_CLASS_CODE'] != itemset[1])]
            dicA[area] = dic[area]  # key 区名 value 区对应的需要插补的数据
            codeFindName(cursor, itemset, today, conn, support, area, flag)
    path = os.path.realpath(sys.argv[0])
    createName = getConf("common", "createName", path)
    createId = getConf("common", "createId", path)
    for i in list(set(wei['Issue_Region_Name'])):
        for k, v in dicA.items():
            if i == k:
                # dfchabu.index = range(len(dfchabu)) # 修改序号
                for index, row in v.iterrows():
                    infoIss = 'select ISSUE_TYPE_NAME from DATAPREDICT_FPM_QL_INFO  WHERE ISSUE_TYPE_CODE = ' + "'" + \
                              row['ISSUE_TYPE_CODE'] + "'and PETITION_DATE_FLAG = " + "'" + flag + "'"
                    logging.info(" w select DATAPREDICT_FPM_QL_INFO infoIss sql: %s\t" % (infoIss))
                    cursor.execute(infoIss)
                    infoIss = cursor.fetchall()
                    # infoObj = 'select OBJECT_CLASS_NAME from DATAPREDICT_FPM_QL_INFO  WHERE OBJECT_CLASS_CODE = ' + "'" +  row['OBJECT_CLASS_CODE'] + "' and PETITION_DATE_FLAG = " + "'" + flag + "'"
                    infoObj = 'select OBJECT_CLASS_NAME from DATAPREDICT_FPM_QL_INFO  WHERE OBJECT_CLASS_CODE = ' + "'" + \
                              row['OBJECT_CLASS_CODE'] + "'and PETITION_DATE_FLAG = " + "'" + flag + "'"
                    cursor.execute(infoObj)
                    infoObj = cursor.fetchall()
                    try:
                        sqlInsert = "insert into DATAPREDICT_FPM_QL_ALL_DATA_INFO(OID, ISSUE_TYPE_CODE, ISSUE_TYPE_NAME, OBJECT_CLASS_CODE, OBJECT_CLASS_NAME, SUPPORT, REGIN_NAME, REGIN_CODE, PETITION_DATE_FLAG,CREATE_DATE,CREATOR_NAME,CREATOR_ID,MODIFY_DATE,MODIFY_NAME,MODIFY_ID) VALUES ( '%s', '%s', '%s', '%s', '%s', %s,'%s','%s','%s', date('%s'), '%s', '%s', date('%s'), '%s', '%s')" % (
                            str(shortuuid.ShortUUID().random(length=20)), row['ISSUE_TYPE_CODE'], infoIss[0][0],
                            row['OBJECT_CLASS_CODE'], infoObj[0][0],
                            0, i, '', flag, today, createName, createId, today, '', '')
                        logging.info(" w insert DATAPREDICT_FPM_QL_ALL_DATA_INFO  sql: %s\t" % (sqlInsert))
                        cursor.execute(sqlInsert)
                    except Exception as e:
                        logging.error("w insert DATAPREDICT_FPM_QL_ALL_DATA_INFO error : %s\t" % (e))
                        conn.rollback()  # 回滚


def summary(cursor, startTime, endTime):
    sql = "SELECT m.ISSUE_REGION_FLAG,m.Issue_Region_Code4,m.Region_Code,m.Petition_Date,m.Petition_Type_Code,m.Source_Code4,m.Object_Class_Code,m.Issue_Type_Code,m.Deal_Type_Code,e.Reality_Code" \
          ",(case WHEN DAY( m.Petition_Date)>=20 THEN substr( (m.Petition_Date +1 months),1 ,7) ELSE substr( m.Petition_Date,1,7) END) year_month " \
          "from ( SELECT aa.*,b.Issue_Type_Code,d.Oid doid,d.Deal_Type_Code from( " \
          "(SELECT o.ISSUE_REGION_FLAG,a.oid aoid,a.Issue_Region_Code,substr(a.Issue_Region_Code,1,4) Issue_Region_Code4,a.Region_Code,a.Petition_Date,a.Petition_Type_Code,substr(a.Petition_Source_Code,1,4) Source_Code4,a.Object_Class_Code " \
          "FROM PETITION_BASIC_INFO a,ORGANIZATION_SET o where substr(a.Region_Code,1,4)=substr(o.Issue_Region_Code,1,4) and a.Region_Code='310000000000' and a.Petition_Class_Code='1' " \
          ") UNION (SELECT o.ISSUE_REGION_FLAG,a.oid aoid,a.Issue_Region_Code,substr(a.Issue_Region_Code,1,4) Issue_Region_Code4,a.Region_Code,a.Petition_Date,a.Petition_Type_Code,substr(a.Petition_Source_Code,1,4) Source_Code4,a.Object_Class_Code " \
          "FROM PETITION_BASIC_INFO a,ORGANIZATION_SET o where substr(a.Issue_Region_Code,1,4)=substr(o.Issue_Region_Code,1,4) and a.Region_Code!='310000000000' and a.Petition_Class_Code='1' " \
          ") ) aa " \
          "left join Petition_Issue_Info b on aa.aOid=b.Petition_Basic_Info_Oid " \
          "left join Petition_Deal_Info d on aa.aOid=d.Petition_Basic_Info_Oid " \
          "where d.valid_flag='0' and aa.Petition_Date >""'" + startTime + "'"" and aa.Petition_Date<""'" + endTime + "'"" and (b.Issue_Type_Code != ' ' and aa.Object_Class_Code != ' ' )) m " \
                                                                                                                      "left join Petition_End_Info e on m.dOid=e.Petition_Deal_Info_Oid"
    logging.info("summary  sql  : %s\t" % (sql))
    # 每次执行一次查询
    cursor.execute(sql)
    rows = cursor.fetchall()
    petition_df = pd.DataFrame(rows)
    # 判断select语句查出的数据是否为空
    if petition_df.empty == True:
        return petition_df
    else:
        petition_df.columns = ['qws', 'Issue_Region_Code4', 'Region_Code', 'Petition_Date', 'Petition_Type_Code',
                               'Source_Code4', 'Object_Class_Code'
            , 'Issue_Type_Code', 'Deal_Type_Code', 'Reality_Code', 'year_month']
        # 合并一些区的数据
        petition_df['Issue_Region_Code4'] = petition_df.Issue_Region_Code4.replace(
            {'3119': '3115', '3103': '3101', '3108': '3106'})
        return petition_df


# 取出所有组织机构
def selectOrganization(cursor):
    logging.info("开始从数据库中读组织机构数据")
    sql = "select substr(Issue_Region_Code,1,4) Issue_Region_Code4,Issue_Region_Name,ISSUE_REGION_FLAG from ORGANIZATION_SET where substr(Issue_Region_Code,1,4) not in ('3119','3103','3108') "
    # 每次执行一次查询
    cursor.execute(sql)
    rows = cursor.fetchall()
    org_df = pd.DataFrame(rows)
    org_df.columns = ['Issue_Region_Code4', 'Issue_Region_Name', 'qws']
    logging.info("成功读出数据")
    return org_df


def mainQLC(cursor, modelCycle, conn, supportCity, supportArea, supportWei):
    try:
        start = time.clock()
        today = datetime.datetime.now().strftime('%Y-%m-%d')  # 当前日期
        cur = datetime.datetime.now()
        startTimeM, endTimeM = monthOfMission(cur, today)  # 一个月的时间
        startTimeQ, endTimeQ = quarterlyOfMission(cur, today)  # 季度的时间
        # startTimeS, endTimeS = sixMonthsOfMission(cur, today)  # 半年的时间
        startTimeY, endTimeY = yearOfMission(cur, today)  # 一年的时间

        dfY = summary(cursor, startTimeY, endTimeY)  # 一年数据
        # dfS = summary(cursor, startTimeS, endTimeS)  # 办年数据
        dfQ = summary(cursor, startTimeQ, endTimeQ)  # 季度数据
        dfM = summary(cursor, startTimeM, endTimeM)  # 一个月数据
        org_df = selectOrganization(cursor)  # 取出所有组织机构
        # 判断四个时间维度数据是否为空，把结果存入数据库(上海市)
        if dfY.empty != True:
            logging.info("开始插入一年数据")
            findCityData(dfY, cursor, today, conn, supportCity, "y")  # 查找上海市一年数据
            petition_dfY = pd.merge(org_df, dfY, on=['Issue_Region_Code4', 'qws'])  # 合并一年数据
            findQuData(petition_dfY, cursor, today, conn, supportWei, "y")  # 查找区一年数据
            findWeiData(petition_dfY, cursor, today, conn, supportArea, "y")  # 查找派驻一年数据
        # if dfS.empty != True:
        #     logging.info("开始插入半年数据")
            # findCityData(dfS, cursor, today, conn, supportCity, "s")  # 查找上海市办年数据
            # petition_dfS = pd.merge(org_df, dfS, on=['Issue_Region_Code4', 'qws'])  # 合并半年数据
            # findQuData(petition_dfS, cursor, today, conn, supportWei, "s")  # 查找区办年数据
            # findWeiData(petition_dfS, cursor, today, conn, supportArea, "s")  # 查找派驻办年数据
        if dfQ.empty != True:
            logging.info("开始插入季度数据")
            findCityData(dfQ, cursor, today, conn, supportCity, "q")  # 查找上海市季度数据
            petition_dfQ = pd.merge(org_df, dfQ, on=['Issue_Region_Code4', 'qws'])  # 合并季度数据
            findQuData(petition_dfQ, cursor, today, conn, supportWei, "q")  # 查找区季度数据
            findWeiData(petition_dfQ, cursor, today, conn, supportArea, "q")  # 查找派驻季度数据
        if dfM.empty != True:
            logging.info("开始插入一个月数据")
            findCityData(dfM, cursor, today, conn, supportCity, "m")  # 查找上海市一个月数据
            petition_dfM = pd.merge(org_df, dfM, on=['Issue_Region_Code4', 'qws'])  # 合并一个月数据
            findQuData(petition_dfM, cursor, today, conn, supportWei, "m")  # 查找区一个月数据
            findWeiData(petition_dfM, cursor, today, conn, supportArea, "m")  # 查找派驻一个月数据

        # 该语句将清除表中所有数据，但由于这一操作会记日志，因此执行速度会相对慢一些，另外要注意的是，
        # 如果表较大，为保证删除操作的成功，应考虑是否留有足够大的日志空间
        yesTime = datetime.datetime.now() + datetime.timedelta(days=-modelCycle)
        yesTimeNyr = yesTime.strftime('%Y-%m-%d')  # 格式化输出 前n天
        sqlDelete = "delete from DATAPREDICT_FPM_QL_ALL_DATA_INFO where substr(CREATE_DATE,1,10)<='" + yesTimeNyr + "'"  # 删表数据
        logging.info(" delete table  sql: %s\t" % (sqlDelete))
        cursor.execute(sqlDelete)  # 执行
        upda = "UPDATE DATAPREDICT_FPM_QL_ALL_DATA_INFO SET OBJECT_CLASS_NAME = '其他' WHERE OBJECT_CLASS_NAME = '其他人员' "
        cursor.execute(upda)  # 执行
        end = time.clock()  # 运行结束时间
        logging.info(u'\n问题类别与行政级别关联分析并分区存储完毕，用时：%0.2f秒' % (end - start))
    except Exception as e:
        logging.error("Exception: %s\t" % (e))
    finally:
        pass
