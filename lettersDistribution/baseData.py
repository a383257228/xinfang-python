# -*- coding: utf-8 -*-
'''
@ author: jh
@ time: 2017/8/17
@ Function: 连接db2
'''
import pandas as pd
import shortuuid
import sys
from commonUtil import *  # 从文件路径为commonUtil的文件中导入所有的函数
import logging

# 获取日志对象
logging = getLog()

reload(sys)
sys.setdefaultencoding('utf-8')


# 从数据库中读数据
def selectData(cursor):
    logging.info("start selectData.")
    # print ('开始从数据库中读数据')
    try:
        sql = "SELECT m.ISSUE_REGION_FLAG,m.Issue_Region_Code4,m.Region_Code,m.Petition_Date,m.Petition_Type_Code,m.Source_Code4,m.Object_Class_Code,m.Issue_Type_Code,m.Deal_Type_Code,e.Reality_Code" \
              ",(case WHEN DAY( m.Petition_Date)>20 THEN substr( (m.Petition_Date +1 months),1 ,7) ELSE substr( m.Petition_Date,1,7) END) year_month " \
              "from ( SELECT aa.*,b.Issue_Type_Code,d.Oid doid,d.Deal_Type_Code from( " \
              "(SELECT o.ISSUE_REGION_FLAG,a.oid aoid,a.Issue_Region_Code,substr(a.Issue_Region_Code,1,4) Issue_Region_Code4,a.Region_Code,a.Petition_Date,a.Petition_Type_Code,substr(a.Petition_Source_Code,1,4) Source_Code4,a.Object_Class_Code " \
              "FROM PETITION_BASIC_INFO a,ORGANIZATION_SET o where substr(a.Region_Code,1,4)=substr(o.Issue_Region_Code,1,4) and a.Region_Code='310000000000' and a.Petition_Class_Code='1' " \
              ") UNION (SELECT o.ISSUE_REGION_FLAG,a.oid aoid,a.Issue_Region_Code,substr(a.Issue_Region_Code,1,4) Issue_Region_Code4,a.Region_Code,a.Petition_Date,a.Petition_Type_Code,substr(a.Petition_Source_Code,1,4) Source_Code4,a.Object_Class_Code " \
              "FROM PETITION_BASIC_INFO a,ORGANIZATION_SET o where substr(a.Issue_Region_Code,1,4)=substr(o.Issue_Region_Code,1,4) and a.Region_Code!='310000000000' and a.Petition_Class_Code='1' " \
              ") ) aa " \
              "left join Petition_Issue_Info b on aa.aOid=b.Petition_Basic_Info_Oid " \
              "left join Petition_Deal_Info d on aa.aOid=d.Petition_Basic_Info_Oid " \
              "where d.valid_flag='0') m " \
              "left join Petition_End_Info e on m.dOid=e.Petition_Deal_Info_Oid "
        # 每次执行一次查询
        cursor.execute(sql)
        rows = cursor.fetchall()
        petition_df = pd.DataFrame(rows)
        petition_df.columns = ['qws', 'Issue_Region_Code4', 'Region_Code', 'Petition_Date', 'Petition_Type_Code',
                               'Source_Code4', 'Object_Class_Code'
            , 'Issue_Type_Code', 'Deal_Type_Code', 'Reality_Code', 'year_month']
        logging.info('selectData:成功读出数据')
        # 合并一些区的数据
        petition_df['Issue_Region_Code4'] = petition_df.Issue_Region_Code4.replace(
            {'3119': '3115', '3103': '3101', '3108': '3106'})
        return petition_df
    except Exception as e:
        logging.info('selectData:读出数据失败')
        logging.error("selectData: %s\t" % (e))
        return None


# 取出所有组织机构
def selectOrganization(cursor):
    try:
        logging.info("start selectOrganization.")
        # print ('开始从数据库中读组织机构数据')
        sql = "select substr(Issue_Region_Code,1,4) Issue_Region_Code4,Issue_Region_Name,ISSUE_REGION_FLAG from ORGANIZATION_SET where substr(Issue_Region_Code,1,4) not in ('3119','3103','3108') "
        # 每次执行一次查询
        cursor.execute(sql)
        rows = cursor.fetchall()
        org_df = pd.DataFrame(rows)
        org_df.columns = ['Issue_Region_Code4', 'Issue_Region_Name', 'qws']
        logging.info('selectOrganization:成功读出数据')
        return org_df
    except Exception as e:
        logging.info('selectOrganization:读出数据失败')
        logging.error("selectOrganization: %s\t" % (e))
        return None


def insert_function(cursor, conn, d, today):
    logging.info("start insert_function.")
    logging.info('insert_function:开始插入数据,共插入' + str(len(d)) + '条数据')
    tableName = 'DATAPREDICT_DISTRIBUTION_INFO'
    path = os.path.realpath(sys.argv[0])
    createName = getConf("common", "createName", path)
    createId = getConf("common", "createId", path)
    modifyName = getConf("common", "modifyName", path)
    modifyId = getConf("common", "modifyId", path)

    for i in range(len(d)):  # ['Issue_Region_Code4','Issue_Region_Name','num','qws','time']
        try:
            sqlInsert = "insert into " + tableName + "(OID,ISSUE_REGION_CODE,ISSUE_REGION_NAME,FORECAST_NUM,ISSUE_REGION_FLAG,FORECAST_DATE,CREATE_DATE,CREATOR_NAME,CREATOR_ID,MODIFY_DATE,MODIFY_NAME,MODIFY_ID) VALUES ('%s', '%s', '%s', %s, %s, date('%s'), timestamp('%s'),'%s','%s', timestamp('%s'),'%s','%s')" % (
                str(shortuuid.ShortUUID().random(length=20)), d.iloc[i, 0], d.iloc[i, 1], d.iloc[i, 2], d.iloc[i, 3],
                d.iloc[i, 4], str(today), createName, createId, str(today), modifyName, modifyId)
            print sqlInsert
            cursor.execute(sqlInsert)
            logging.info('insert_function:成功插入' + str(i + 1) + '条数据')
        except Exception as e:
            logging.error("insert_function: %s\t" % (e))
            conn.rollback()  # 回滚
