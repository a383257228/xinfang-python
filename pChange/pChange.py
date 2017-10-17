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

reload(sys)
sys.setdefaultencoding('utf-8')


# 统计区委自办转办数量
def summaryQW(cursor):
    sqlQW = "select b1.qws,b1.Issue_Region_Code4,b1.num1,b2.num2 from(select m.qws,m.Issue_Region_Code4,count(1) num1 from (SELECT substr(a.Issue_Region_Code,1,4) Issue_Region_Code4 ,(case WHEN (a.Region_Code='310000000000') THEN 's'  WHEN (substr(a.Issue_Region_Code,1,4) in ('3101','3104','3105','3106','3107','3109','3110','3112','3113','3114','3115','3116','3117','3118','3120','3130','3119','3103','3108') ) THEN 'q'  ELSE 'w' END) qws FROM DB2ADMIN.PETITION_BASIC_INFO a where a.Issue_Region_Code like '31%'  and a.Petition_Class_Code='1'  and substr(a.Petition_Source_Code,1,2)='01') m where m.qws!='s' group by m.qws,m.Issue_Region_Code4) b1 ,(select m.qws,m.Issue_Region_Code4,count(1) num2 from(SELECT substr(a.Issue_Region_Code,1,4) Issue_Region_Code4 ,(case WHEN (a.Region_Code='310000000000') THEN 's'  WHEN (substr(a.Issue_Region_Code,1,4) in ('3101','3104','3105','3106','3107','3109','3110','3112','3113','3114','3115','3116','3117','3118','3120','3130','3119','3103','3108') ) THEN 'q'  ELSE 'w' END) qws FROM DB2ADMIN.PETITION_BASIC_INFO a where a.Issue_Region_Code like '31%'  and a.Petition_Class_Code='1'  and substr(a.Petition_Source_Code,1,2)!='01') m where m.qws!='s' group by m.qws,m.Issue_Region_Code4) b2 where b1.Issue_Region_Code4=b2.Issue_Region_Code4 and b1.qws=b2.qws"
    cursor.execute(sqlQW)
    rows = cursor.fetchall()
    dfQW = pd.DataFrame(rows)
    dfQW.columns = ['Issue_Region_Flag', 'Issue_Region_Code4', 'self', 'turn']
    dfQW['Issue_Region_Code4'] = dfQW.Issue_Region_Code4.replace(
        {'3119': '3115', '3103': '3101', '3108': '3106'})  # 合并区
    dfQW = dfQW.groupby(['Issue_Region_Flag', 'Issue_Region_Code4'], as_index=False)['self', 'turn'].sum()
    return dfQW


# 统计市自办转办数量
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


def mainQC(cursor, conn, modelCycle):
    try:
        start = time.clock()
        today = datetime.datetime.now().strftime('%Y-%m-%d')  # 当前日期
        dfQW = summaryQW(cursor)
        dfS = summaryS(cursor)
        org_df = selectOrganization(cursor)  # 取出所有组织机构
        petition_df = pd.merge(org_df, dfQW, on=['Issue_Region_Code4'])  # 合并 查区域名字
        for index, row in petition_df.iterrows():
            try:
                sqlInsert = "insert into DATAPREDICT_QC_INFO(OID, SELF, TURN, Issue_Region_Name,Issue_Region_Code4, Issue_Region_Flag, CATEGORYDATA, CREATE_DATE, CREATOR_NAME, CREATOR_ID, MODIFY_DATE, MODIFY_NAME, MODIFY_ID) VALUES ('%s',%s,%s,'%s','%s','%s',date('%s'), date('%s'), '%s', '%s', date('%s'), '%s', '%s')" % (
                    str(shortuuid.ShortUUID().random(length=20)), row["self"], row["turn"], row["Issue_Region_Name"],
                    row["Issue_Region_Code4"], row["Issue_Region_Flag"], today, today, '', '', str(today), '', '')
                cursor.execute(sqlInsert)
            except Exception as e:
                print e
                conn.rollback()  # 回滚
        for index, row in dfS.iterrows():
            try:
                sqlInsert = "insert into DATAPREDICT_QC_INFO(OID, SELF,TURN, Issue_Region_Name,Issue_Region_Code4,Issue_Region_Flag, CATEGORYDATA, CREATE_DATE, CREATOR_NAME, CREATOR_ID, MODIFY_DATE, MODIFY_NAME, MODIFY_ID) VALUES ('%s',%s,%s,'%s','%s', '%s',  date('%s'), date('%s'), '%s', '%s', date('%s'), '%s', '%s')" % (
                    str(shortuuid.ShortUUID().random(length=20)), row["self"], row["turn"], '上海市', '',
                    row["Issue_Region_Flag"], today, today, '', '', str(today), '', '')
                cursor.execute(sqlInsert)
            except Exception as e:
                print e
                conn.rollback()  # 回滚
        # 该语句将清除表中所有数据，但由于这一操作会记日志，因此执行速度会相对慢一些，另外要注意的是，
        # 如果表较大，为保证删除操作的成功，应考虑是否留有足够大的日志空间
        yesTime = datetime.datetime.now() + datetime.timedelta(days=-modelCycle)
        yesTimeNyr = yesTime.strftime('%Y-%m-%d')  # 格式化输出 前n天
        sqlDelete = "delete from DATAPREDICT_QUESTION_CHANGE_INFO where CATEGORYDATA<='" + yesTimeNyr + "'"  # 删表数据
        cursor.execute(sqlDelete)  # 执行
        end = time.clock()  # 运行结束时间
        print(u'\n存储完毕，用时：%0.2f秒' % (end - start))  # 打印运行总时间
    except Exception as e:
        print e  # 打印异常
    finally:
        # cursor.close()  # 关闭连接
        pass
