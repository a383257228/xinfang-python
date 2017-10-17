# -*- coding: utf-8 -*-
'''
@author: liuzd
@time: 2017/8/24 09:00
@Function: top5
'''
import pandas as pd
import datetime
import sys
import fp_growth as fp
import time
import shortuuid

reload(sys)
sys.setdefaultencoding('utf-8')


# 统计区委问题top5
def summaryQW(cursor, startTimeM, endTimeM):
    sqlQW = "select mm.qws,mm.Issue_Region_Code4,mm.Petition_Date10,mm.oid from(select m.qws,m.Issue_Region_Code4,m.Petition_Date10,m.oid,count(1) co from(SELECT substr(a.Issue_Region_Code,1,4) Issue_Region_Code4,a.oid,substr(a.Petition_Date,1,10) Petition_Date10 ,(case WHEN (a.Region_Code='310000000000') THEN 's'  WHEN (substr(a.Issue_Region_Code,1,4) in ('3101','3104','3105','3106','3107','3109','3110','3112','3113','3114','3115','3116','3117','3118','3120','3130','3119','3103','3108') ) THEN 'q'  ELSE 'w' END) qws FROM DB2ADMIN.PETITION_BASIC_INFO a left join ISSUE_TYPE_INFO b on a.Oid=b.Petition_Basic_Info_Oid where a.Issue_Region_Code like '31%'  and a.Petition_Class_Code='1' and a.Petition_Date >""'" + startTimeM + "'"" and a.Petition_Date<""'" + endTimeM + "'"" ) m where m.qws!='s' group by m.qws,m.Issue_Region_Code4,m.Petition_Date10,m.oid)mm where mm.co >1"
    print sqlQW
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
def summaryS(cursor):
    sqlQW = "select b1.qws,b1.num1,b2.num2 from (select m.qws,count(1) num1 from(SELECT substr(a.Issue_Region_Code,1,4) Issue_Region_Code4,(case WHEN (a.Region_Code='310000000000') THEN 's'  WHEN (substr(a.Issue_Region_Code,1,4) in ('3101','3104','3105','3106','3107','3109','3110','3112','3113','3114','3115','3116','3117','3118','3120','3130','3119','3103','3108') ) THEN 'q'  ELSE 'w' END) qws FROM DB2ADMIN.PETITION_BASIC_INFO a where a.Issue_Region_Code like '31%'  and a.Petition_Class_Code='1'  and substr(a.Petition_Source_Code,1,2)='01') m where m.qws='s' group by m.qws) b1 ,(select m.qws,count(1) num2 from (SELECT substr(a.Issue_Region_Code,1,4) Issue_Region_Code4,(case WHEN (a.Region_Code='310000000000') THEN 's'  WHEN (substr(a.Issue_Region_Code,1,4) in  ('3101','3104','3105','3106','3107','3109','3110','3112','3113','3114','3115','3116','3117','3118','3120','3130','3119','3103','3108') ) THEN 'q'  ELSE 'w' END) qws FROM DB2ADMIN.PETITION_BASIC_INFO a where a.Issue_Region_Code like '31%'  and a.Petition_Class_Code='1'  and substr(a.Petition_Source_Code,1,2)!='01') m where m.qws='s' group by m.qws) b2 where b1.qws=b2.qws"
    cursor.execute(sqlQW)
    rows = cursor.fetchall()
    dfS = pd.DataFrame(rows)
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


# ##################     一年的时间(判断是否是任务月) 每月20号为任务月    ################## #
def yearOfMission(cur, today):
    if cur.month < 12 and cur.day <= 20:
        startTime = (datetime.datetime(cur.year - 1, 12, 20)).strftime("%Y-%m-%d")
        endTime = today
        return startTime, endTime
    if cur.month == 12 and cur.day <= 20:
        startTime = (datetime.datetime(cur.year - 1, cur.month, 20)).strftime("%Y-%m-%d")
        endTime = today
        return startTime, endTime
    if cur.month == 12 and cur.day > 20:
        startTime = (datetime.datetime(cur.year, cur.month, 20)).strftime("%Y-%m-%d")
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


def mainTop5(cursor, conn, modelCycle):
    try:
        start = time.clock()
        today = datetime.datetime.now().strftime('%Y-%m-%d')  # 当前日期
        cur = datetime.datetime.now()
        startTimeM, endTimeM = monthOfMission(cur, today)  # 一个月的时间
        startTimeQ, endTimeQ = quarterlyOfMission(cur, today)  # 季度的时间
        startTimeS, endTimeS = sixMonthsOfMission(cur, today)  # 半年的时间
        startTimeY, endTimeY = yearOfMission(cur, today)  # 一年的时间
        dfQWM = summaryQW(cursor, startTimeM, endTimeM)  # 统计区委问题top5
        dfQWQ = summaryQW(cursor, startTimeQ, endTimeQ)
        dfQWS = summaryQW(cursor, startTimeS, endTimeS)
        dfQWY = summaryQW(cursor, startTimeY, endTimeY)
        org_df = selectOrganization(cursor)  # 取出所有组织机构
        # 判断df是否为空，即根据时间取的数据是否为空（月，季度，半年，年）
        if dfQWM.empty == True:
           pass
        else:
            petition_dfM = pd.merge(org_df, dfQWM, on=['Issue_Region_Code4'])  # 合并 查区域名

        if dfQWQ.empty == True:
            pass
        else:
            petition_dfQ = pd.merge(org_df, dfQWQ, on=['Issue_Region_Code4'])  # 合并 查区域名

        if dfQWS.empty == True:
            pass
        else:
            petition_dfS = pd.merge(org_df, dfQWS, on=['Issue_Region_Code4'])  # 合并 查区域名

        if dfQWY.empty == True:
            pass
        else:
            petition_dfY = pd.merge(org_df, dfQWY, on=['Issue_Region_Code4'])  # 合并 查区域名
            lisAll = []
            for index, row in petition_dfY.iterrows():
                print row
                try:
                    sqlSelect = 'select ISSUE_TYPE_NAME from ISSUE_TYPE_INFO where PETITION_BASIC_INFO_OID = ' + "'" + row[
                        'oid'] + "'"
                    cursor.execute(sqlSelect)
                    rows = cursor.fetchall()
                    pro = pd.DataFrame(rows)
                    lis = []
                    for i in pro.iloc[:, 0]:
                        lis.append(i)
                        lisAll.append(lis)
                except Exception as e:
                    print e
            resultCity = []
            for itemset, support in fp.find_frequent_itemsets(lisAll, 0, True):
                resultCity.append((itemset, support))
            for itemset, support in resultCity:
                # 判断list长度，过滤不符合条件的集合
                if (len(itemset) == 2):
                    pass
                    # print itemset
                if (len(itemset) == 1): # 统计问题top5
                    print itemset[0]
                    try:
                        sqlInsert = "insert into DATAPREDICT_TOP5_INFO(OID, ISSUE_TYPE_CODE, ISSUE_TYPE_NAME, SUPPORT,regin_code4,regin_name, CATEGORYDATA,CREATE_DATE,CREATOR_NAME,CREATOR_ID,MODIFY_DATE,MODIFY_NAME,MODIFY_ID) VALUES ( '%s', '%s', '%s', %s, '%s', '%s', date('%s'), date('%s'), '%s', '%s', date('%s'), '%s', '%s')" % (
                            str(shortuuid.ShortUUID().random(length=20)), str(itemset[0]),  str(itemset[1]),
                             support, '222','name', str(today), str(today), '', '', str(today), '', '')
                        cursor.execute(sqlInsert)
                    except Exception as e:
                        print e
                        conn.rollback()  # 回滚
        # try:
        #     sqlInsert = "insert into DATAPREDICT_TOP5_INFO(OID, ISSUE_REGION_FLAG,ISSUE_REGION_CODE4, Issue_Region_Name, PETITION_DATE, CREATE_DATE, CREATOR_NAME, CREATOR_ID, MODIFY_DATE, MODIFY_NAME, MODIFY_ID) VALUES ('%s','%s','%s', '%s',  date('%s'), date('%s'), '%s', '%s', date('%s'), '%s', '%s')" % (
        #         str(row["oid"]), str(row['Issue_Region_Flag']), str(row['Issue_Region_Code4']),
        #         str(row["Issue_Region_Name"]), str(row['data']), today, '', '', str(today), '', '')
        #     cursor.execute(sqlInsert)
        # except Exception as e:
        #     print e
        #     conn.rollback()  # 回滚
        # 该语句将清除表中所有数据，但由于这一操作会记日志，因此执行速度会相对慢一些，另外要注意的是，
        # 如果表较大，为保证删除操作的成功，应考虑是否留有足够大的日志空间
        # yesTime = datetime.datetime.now() + datetime.timedelta(days=-modelCycle)
        # yesTimeNyr = yesTime.strftime('%Y-%m-%d')  # 格式化输出 前n天
        # sqlDelete = "delete from DATAPREDICT_QUESTION_CHANGE_INFO where CATEGORYDATA<='" + yesTimeNyr + "'"  # 删表数据
        # cursor.execute(sqlDelete)  # 执行
        end = time.clock()  # 运行结束时间
        print(u'\n存储完毕，用时：%0.2f秒' % (end - start))  # 打印运行总时间
    except Exception as e:
        print e  # 打印异常
    finally:
        # cursor.close()  # 关闭连接
        pass
