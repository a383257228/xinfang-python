# -*- coding: utf-8 -*-
'''
@ author: liyang
@ time: 2017/8/17
@民意分析 查询数据库方法
'''
# 引入 word2vec
import sys
import pandas as pd
import shortuuid
import datetime
import time
import logging
import os
from commonUtil import getLog # 从文件路径为connectionDB2的文件中导入所有的函数
from commonUtil import getConf
from commonUtil import getConfInt
reload(sys)
sys.setdefaultencoding( "utf-8" )

# 获取日志对象
logging = getLog()
'''
获取某一区的所有历史数据
'''
def getDfByQuIssueCode(cursor) :
    try:
        sql = "select i.Region_Code,i.Region_Name,i.Issue_Type_Code,i.Issue_Type_Name,i.Key_Word_Content,i.ISSUE_CONTENT_NUM_FLAG, c.Issue_Content cic ,i.Issue_Content  iic from PETITION_ISSUE_INFO i , Issue_Content_Info c where c.Petition_Issue_Info_Oid = i.OID"
        # sql = "select ISSUE_CONTENT from PETITION_ISSUE_INFO "
        cursor.execute(sql)
        rows = cursor.fetchmany(10)
        # rows = cursor.fetchall()
        if rows != []:
           petition_df = pd.DataFrame(rows)
           return petition_df
        else :
           return None
    except Exception as ex:
        logging.error("getDfByQuIssueCode Failed : %s\t" % (ex))
        return None


'''
获取文本数据
start:开始下标
end:结束下标
'''
def getDfContent(cursor,start,end) :
    try:
        # sql = "select * from (select ROW_NUMBER() OVER(ORDER BY oid DESC) AS ROWNUM, FILE_CONTENT ,IIC,CIC from DATAPREDICT_OPINION_CONTEXT ) where ROWNUM >= %s and ROWNUM <= %s" % (start,end)
        # sql = "select * from (select ROW_NUMBER() OVER(ORDER BY oid DESC) AS ROWNUM,ISSUE_REGION_FLAG,OID,substr(Region_Code,1,4) REGION_CODE,REGION_NAME,substr(ISSUE_REGION_CODE,1,4) ISSUE_REGION_CODE,ISSUE_REGION_NAME,OID AS BOID, FILE_CONTENT ,IIC,CIC from DATAPREDICT_OPINION_CONTEXT ) where  ROWNUM >= %s and ROWNUM <= %s" % (
        # start, end)

        sql = "select * from (select ROW_NUMBER() OVER(ORDER BY ba.oid DESC) AS ROWNUM,ba.*,al.*  from ( (select o.ISSUE_REGION_FLAG,a.oid as oid,substr(a.REGION_CODE,1,4) REGION_CODE ,a.REGION_NAME,substr(a.ISSUE_REGION_CODE,1,4) ISSUE_REGION_CODE,a.ISSUE_REGION_NAME FROM PETITION_BASIC_INFO a,ORGANIZATION_SET o where (substr(a.Region_Code,1,4)=substr(o.Issue_Region_Code,1,4) and a.Region_Code='310000000000' and a.Petition_Class_Code='1')) union (select o.ISSUE_REGION_FLAG,a.oid as oid,substr(a.REGION_CODE,1,4) REGION_CODE,a.REGION_NAME,substr(a.ISSUE_REGION_CODE,1,4) ISSUE_REGION_CODE,a.ISSUE_REGION_NAME FROM PETITION_BASIC_INFO a,ORGANIZATION_SET o where(substr(a.Issue_Region_Code,1,4)=substr(o.Issue_Region_Code,1,4) and a.Region_Code!='310000000000' and a.Petition_Class_Code='1'))) ba  left join (select i.PETITION_BASIC_INFO_OID AS BOID,s.FILE_CONTENT,i.ISSUE_CONTENT iic, c.ISSUE_CONTENT cic from PETITION_ISSUE_INFO i left join PETITION_SCAN_FILE s on i.PETITION_NO = s. PETITION_NO and i.REGION_CODE = s.REGION_CODE left join ISSUE_CONTENT_INFO c on i.oid = c.PETITION_ISSUE_INFO_OID) al   on ba.OID = al.BOID ) where ROWNUM >= %s and ROWNUM <= %s" % (start,end)


        # sql = "select * from (select ROW_NUMBER() OVER(ORDER BY i.oid DESC) AS ROWNUM,s.FILE_CONTENT,i.ISSUE_CONTENT iic, c.ISSUE_CONTENT cic from PETITION_ISSUE_INFO i left join PETITION_SCAN_FILE s on i.PETITION_NO = s. PETITION_NO and i.REGION_CODE = s.REGION_CODE left join ISSUE_CONTENT_INFO c on i.oid = c.PETITION_ISSUE_INFO_OID )where ROWNUM >= %s and ROWNUM <= %s" % (start,end)
        print sql
        cursor.execute(sql)
        logging.info(sql)
        # rows = cursor.fetchmany(5)
        rows = cursor.fetchall()
        if rows != []:
            # petition_df = pd.DataFrame(rows)
            # petition_df.columns=['ROWNUM','FILE_CONTENT' ,'IIC','CIC']
            # return petition_df

            petition_df = pd.DataFrame(rows)
            # petition_df.columns=['ROWNUM','FILE_CONTENT' ,'IIC','CIC']
            petition_df.columns = ['ROWNUM', 'ISSUE_REGION_FLAG', 'OID', 'REGION_CODE', 'REGION_NAME',
                                   'ISSUE_REGION_CODE', 'ISSUE_REGION_NAME', 'BOID', 'FILE_CONTENT', 'IIC', 'CIC', ]
            # 替换编码
            petition_df['ISSUE_REGION_CODE'] = petition_df.ISSUE_REGION_CODE.replace(
                {'3119': '3115', '3103': '3101', '3108': '3106'})
            # 替换名称
            petition_df['ISSUE_REGION_NAME'] = petition_df.ISSUE_REGION_NAME.replace(
                {'南汇区': '浦东新区', '卢湾区': '黄浦区', '闸北区': '静安区'})
            # print petition_df
            return petition_df
        else :
            return None
    except Exception as ex:
        logging.error("getDfContent Failed : %s\t" % (ex))
        return None


'''
获取所有文本的总条数
'''
def getAllContextNum(cursor):
    try:
        # sql = "select count(*) counts from DATAPREDICT_OPINION_CONTEXT "
        # sql = "select count(*) counts from PETITION_ISSUE_INFO i left join PETITION_SCAN_FILE s on i.PETITION_NO = s. PETITION_NO and i.REGION_CODE = s.REGION_CODE left join ISSUE_CONTENT_INFO c on i.oid = c.PETITION_ISSUE_INFO_OID"
        sql = "select count(*) counts  from ( (select o.ISSUE_REGION_FLAG,a.oid as oid,substr(a.REGION_CODE,1,4) REGION_CODE ,a.REGION_NAME,substr(a.ISSUE_REGION_CODE,1,4) ISSUE_REGION_CODE,a.ISSUE_REGION_NAME FROM PETITION_BASIC_INFO a,ORGANIZATION_SET o where (substr(a.Region_Code,1,4)=substr(o.Issue_Region_Code,1,4) and a.Region_Code='310000000000' and a.Petition_Class_Code='1')) union (select o.ISSUE_REGION_FLAG,a.oid as oid,substr(a.REGION_CODE,1,4) REGION_CODE,a.REGION_NAME,substr(a.ISSUE_REGION_CODE,1,4) ISSUE_REGION_CODE,a.ISSUE_REGION_NAME FROM PETITION_BASIC_INFO a,ORGANIZATION_SET o where(substr(a.Issue_Region_Code,1,4)=substr(o.Issue_Region_Code,1,4) and a.Region_Code!='310000000000' and a.Petition_Class_Code='1'))) ba  left join (select i.PETITION_BASIC_INFO_OID AS BOID,s.FILE_CONTENT,i.ISSUE_CONTENT iic, c.ISSUE_CONTENT cic from PETITION_ISSUE_INFO i left join PETITION_SCAN_FILE s on i.PETITION_NO = s. PETITION_NO and i.REGION_CODE = s.REGION_CODE left join ISSUE_CONTENT_INFO c on i.oid = c.PETITION_ISSUE_INFO_OID) al   on ba.OID = al.BOID "
        cursor.execute(sql)
        rows = cursor.fetchall()
        if rows != []:
            petition_df = pd.DataFrame(rows)
            petition_df.columns=['COUNTS']
            return petition_df
        else :
            return None
    except Exception as ex:
        logging.error("getAllContextNum Failed : %s\t" % (ex))
        return None




'''
民意分析新词更新到数据库里  用来页面做选择
'''
def updateOpiAnaly (cursor,dealUuid) :

    path = os.path.realpath(sys.argv[0])
    topNum = getConfInt("opinion", "topNum", path)
    try:
        sql = "select * from (SELECT ROW_NUMBER() OVER(ORDER BY sum(WORD_FREQUENCY) DESC) AS ROWNUM,  NEW_WORD, sum(WORD_FREQUENCY) counts, WORD_PART  FROM DATAPREDICT_OPINION_WORD  group by NEW_WORD,WORD_PART)where ROWNUM >= 1 and ROWNUM <= %s;" % (topNum)
        cursor.execute(sql)
        rows = cursor.fetchall()
        if rows != []:
            # 有新词 则处理之前的词
            if len(dealUuid) > 0:
                try:
                    # 更新数据库 更新词状态为处理
                    updateDealFlag(cursor, dealUuid)
                    logging.info("updateDealFlag Success!")
                except Exception as ex:
                    logging.error("save to userDict Failed : %s\t" % (ex))

            # 删除数据库里的无用词典
            deleteOtherWord(cursor)

            # 数据库写入新词
            petition_df = pd.DataFrame(rows)
            petition_df.columns=['ROWNUM','NEW_WORD','WORD_FREQUENCY','WORD_PART']
            # 创建人信息
            # path = os.path.realpath(sys.argv[0])
            createName = getConf("common", "createName", path)
            createId = getConf("common", "createId", path)
            for index, row in petition_df.iterrows():   # 获取每行的index、row
               uuid = shortuuid.ShortUUID().random(length=20)
               data = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
               # time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
               sql= "INSERT INTO DATAPREDICT_OPINION_INFO (OID, NEW_WORD, WORD_FREQUENCY, WORD_PART, EDIT_WORD, DICTIONARY_FLAG, IGNORE_FLAG, DEAL_FLAG,WORD_USED_FLAG , CREATE_DATE, CREATOR_NAME, CREATOR_ID, MODIFY_DATE, MODIFY_NAME, MODIFY_ID,CODE_TYPE) " \
                 "VALUES ('%s', '%s', %s, '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', '%s', '%s','%s');" \
                     % (uuid,row['NEW_WORD'],row['WORD_FREQUENCY'],row['WORD_PART'],row['NEW_WORD'],'0','0','0','0',data,createName,createId,data,'','','WTXZ')

               cursor.execute(sql)
            logging.info("updateWord Success!")

    except Exception as ex:
        logging.error("updateOpiAnaly Failed: %s\t" % (ex))


'''
民意分析更新词频
'''

def updateWordFrequency(cursor):
   try :
       sql = "SELECT NEW_WORD, SUM(WORD_FREQUENCY) COUNTS   FROM DATAPREDICT_OPINION_OLD_WORD  GROUP BY NEW_WORD,WORD_FREQUENCY"
       cursor.execute(sql)
       rows = cursor.fetchall()
       if rows != []:
           # 创建人信息
           path = os.path.realpath(sys.argv[0])
           modifyName = getConf("common", "modifyName", path)
           modifyId = getConf("common", "modifyId", path)
           petition_df = pd.DataFrame(rows)
           petition_df.columns = ['NEW_WORD', 'WORD_FREQUENCY']

           for index, row in petition_df.iterrows():  # 获取每行的index、row
               # 查询词信息
               sqlGet = "SELECT OID,WORD_FREQUENCY from DATAPREDICT_OPINION_INFO WHERE NEW_WORD = '%s' " % (row['NEW_WORD'])
               cursor.execute(sqlGet)
               rowsW = cursor.fetchall()
               if(rowsW != []) :
                   petition_dfW = pd.DataFrame(rowsW)
                   petition_dfW.columns = ['OID','WORD_FREQUENCY']
                   data = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
                   #更新词频
                   for indexW, rowW in petition_dfW.iterrows():
                       sqlW = "UPDATE DATAPREDICT_OPINION_INFO SET WORD_FREQUENCY = '%s' ,MODIFY_DATE = '%s', MODIFY_NAME = '%s', MODIFY_ID = '%s' where OID = '%s'" %(row['WORD_FREQUENCY'],data,modifyName,modifyId,rowW['OID'])
                       cursor.execute(sqlW)

       logging.info("updateWordFrequency Success!")
   except Exception as ex:
       logging.error("updateWordFrequency Failed: %s\t" % (ex))


'''
民意分析新词更新到数据库里  用来页面做选择
'''
def updateOpinionWord (cursor,newWords,rowAll) :
    try:
        newWords =  newWords[:-1] #截取从头开始到倒数第三个字符之前
        arr = newWords.split(",")
        # 创建人信息
        path = os.path.realpath(sys.argv[0])
        createName = getConf("common", "createName", path)
        createId = getConf("common", "createId", path)
        for ai in arr :
            # 查询新的ISSUE_REGION_NAME
            sql = "SELECT ISSUE_REGION_NAME FROM ORGANIZATION_SET where ISSUE_REGION_CODE like '"+rowAll.get("ISSUE_REGION_CODE")+"%'"
            cursor.execute(sql)
            rowsWp = cursor.fetchall()
            if (rowsWp != []):
                petition_dfWp = pd.DataFrame(rowsWp)
                petition_dfWp.columns = ['ISSUE_REGION_NAME']
                # print petition_dfWp
                ISSUE_REGION_NAME = petition_dfWp.iat[0,0]
                # print ISSUE_REGION_NAME
                # 插入新词
                aiArr = ai.split(" ")
                uuid = shortuuid.ShortUUID().random(length=20)
                data = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
                # time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                sql= "INSERT INTO DATAPREDICT_OPINION_WORD (OID, NEW_WORD, WORD_FREQUENCY, WORD_PART,CREATE_DATE, CREATOR_NAME, CREATOR_ID, MODIFY_DATE, MODIFY_NAME, MODIFY_ID,ISSUE_REGION_FLAG,REGION_CODE,REGION_NAME,ISSUE_REGION_CODE,ISSUE_REGION_NAME,PETITION_BASIC_INFO_OID) " \
                 "VALUES ('%s', '%s', %s, '%s','%s', '%s', '%s','%s', '%s', '%s', %s, '%s','%s', '%s', '%s', '%s');" \
                     % (uuid,aiArr[0],aiArr[2],aiArr[1],data,createName,createId,data,'','',int(rowAll.get("ISSUE_REGION_FLAG")),rowAll.get("REGION_CODE"),rowAll.get("REGION_NAME"),rowAll.get("ISSUE_REGION_CODE"),ISSUE_REGION_NAME,rowAll.get("PETITION_BASIC_INFO_OID"))
                # logging.info(sql)
                cursor.execute(sql)
    except Exception, ex:
        logging.error("updateOpinionWord Failed :  %s\t" % (ex))
# def updateOpinionWord (cursor,newWords) :
#     try:
#         newWords =  newWords[:-1] #截取从头开始到倒数第三个字符之前
#         arr = newWords.split(",")
#         # 创建人信息
#         path = os.path.realpath(sys.argv[0])
#         createName = getConf("common", "createName", path)
#         createId = getConf("common", "createId", path)
#         for ai in arr :
#             aiArr = ai.split(" ")
#             uuid = shortuuid.ShortUUID().random(length=20)
#             data = str(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
#             # time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
#             sql= "INSERT INTO DATAPREDICT_OPINION_WORD (OID, NEW_WORD, WORD_FREQUENCY, WORD_PART,CREATE_DATE, CREATOR_NAME, CREATOR_ID, MODIFY_DATE, MODIFY_NAME, MODIFY_ID) " \
#              "VALUES ('%s', '%s', %s, '%s','%s', '%s', '%s','%s', '%s', '%s');" \
#                  % (uuid,aiArr[0],aiArr[2],aiArr[1],data,createName,createId,data,'','')
#             cursor.execute(sql)
#     except Exception as ex:
#         logging.error("updateOpinionWord Failed :  %s\t" % (ex))


'''
民意分析新词更新到数据库里  更新词频使用
'''

def updateOpinionOldWord(cursor, newWords):
    # 创建人信息
    path = os.path.realpath(sys.argv[0])
    createName = getConf("common", "createName", path)
    createId = getConf("common", "createId", path)
    try :
        newWords = newWords[:-1]  # 截取从头开始到倒数第三个字符之前
        arr = newWords.split(",")
        for ai in arr:
            aiArr = ai.split(" ")
            uuid = shortuuid.ShortUUID().random(length=20)
            data = str(time.strftime('%Y-%m-%d',time.localtime(time.time())))
            # time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            sql = "INSERT INTO DATAPREDICT_OPINION_OLD_WORD (OID, NEW_WORD, WORD_FREQUENCY, WORD_PART,CREATE_DATE, CREATOR_NAME, CREATOR_ID, MODIFY_DATE, MODIFY_NAME, MODIFY_ID) " \
                  "VALUES ('%s', '%s', %s, '%s','%s', '%s', '%s','%s', '%s', '%s');" \
                  % (uuid, aiArr[0], aiArr[2], aiArr[1],data,createName,createId,data,'','')
            # sql.encode('utf8')
            cursor.execute(sql)
    except Exception as ex:
        logging.error("updateOpinionOldWord Failed : %s\t" % (ex))


'''
民意分析读取页面选好的分类好的新词  更新到词典里
'''
def updateDictByNewWord (cursor,regionCode) :
    try:
        sql = "SELECT DICT_URL,NAME_DICT_URL,NEW_WORD,NEW_WORD_DICT,REGION_CODE,REGION_NAME FROM  OPINION_ANALYSIS  WHERE REGION_CODE = '%s' AND PAGE_FLAG = '2';" % (str(regionCode))
        cursor.execute(sql)
        rows = cursor.fetchall()
        if rows != []:
            petition_df = pd.DataFrame(rows)
            petition_df.columns=['DICT_URL','NAME_DICT_URL','NEW_WORD','NEW_WORD_DICT','REGION_CODE','REGION_NAME']
            return  petition_df
            # print petition_df.iat[0,2]   #取data的第一行 #取data的第一列
        else :
            return  None
    except Exception as ex:
        logging.error("updateDictByNewWord Failed : %s\t" % (ex))
        return None
'''
获取词典信息
'''
def getDictInfo (cursor) :
    try:
        # sql = "SELECT STOP_WORD_DICT_URL, TEMPORARY_WORD_DICT_URL, NEW_DICT_NUM, MAX_DICT_NUM,CLASS_WORD_DICT_URL FROM DATAPREDICT_OPINION_DICT;"
        sql = "SELECT STOP_WORD_DICT_URL, TEMPORARY_WORD_DICT_URL, CLASS_WORD_DICT_URL, CODE FROM DATAPREDICT_OPINION_DICT where HAS_CHILD_FLAG = '0';"
        cursor.execute(sql)
        rows = cursor.fetchall()
        if rows != []:
            petition_df = pd.DataFrame(rows)
            petition_df.columns=['STOP_WORD_DICT_URL', 'TEMPORARY_WORD_DICT_URL','CLASS_WORD_DICT_URL','CODE']
            # print  petition_df
            return  petition_df
        else :
            return None
    except Exception as ex:
        logging.error("getDictInfo Failed: %s\t" % (ex))
        return None


'''
获取词典数量
'''
def getDictNum (cursor) :
    # sql = "SELECT STOP_WORD_DICT_URL, TEMPORARY_WORD_DICT_URL, NEW_DICT_NUM, MAX_DICT_NUM,CLASS_WORD_DICT_URL FROM DATAPREDICT_OPINION_DICT;"
    try:
        sql = "SELECT max(DICT_TYPE) FROM DATAPREDICT_OPINION_DICT"
        cursor.execute(sql)
        rows = cursor.fetchall()
        if rows != []:
            petition_df = pd.DataFrame(rows)
            petition_df.columns=['DICT_TYPE']
            # print  petition_df
            return  petition_df
        else :
            return None
    except Exception as ex:
        logging.error("getDictInfo Failed : %s\t" % (ex))
        return None
'''
获取词信息
'''
def getWordInfo (cursor) :
    try:
        # sql = "SELECT STOP_WORD_DICT_URL, TEMPORARY_WORD_DICT_URL, NEW_DICT_NUM, MAX_DICT_NUM,CLASS_WORD_DICT_URL FROM DATAPREDICT_OPINION_DICT;"
        sql = "SELECT OID, NEW_WORD, WORD_FREQUENCY, WORD_PART, EDIT_WORD, DICTIONARY_FLAG, IGNORE_FLAG, DEAL_FLAG, WORD_USED_FLAG FROM DATAPREDICT_OPINION_INFO WHERE DEAL_FLAG != '1';"
        cursor.execute(sql)
        rows = cursor.fetchall()
        if rows != []:
            petition_df = pd.DataFrame(rows)
            petition_df.columns=['OID', 'NEW_WORD', 'WORD_FREQUENCY', 'WORD_PART', 'EDIT_WORD', 'DICTIONARY_FLAG', 'IGNORE_FLAG', 'DEAL_FLAG', 'WORD_USED_FLAG']
            # print  petition_df
            return  petition_df
        return None
    except Exception as ex:
        logging.error("getWordInfo Failed : %s\t" % (ex))
        return None
'''
更新数据库 更新词状态为处理
'''
def updateDealFlag (cursor,oids) :
    # 创建人信息
    path = os.path.realpath(sys.argv[0])
    modifyName = getConf("common", "modifyName", path)
    modifyId = getConf("common", "modifyId", path)
    data = str(time.strftime('%Y-%m-%d', time.localtime(time.time())))
    try:
        sql = "update DATAPREDICT_OPINION_INFO set DEAL_FLAG = '1' ,MODIFY_DATE = '%s', MODIFY_NAME = '%s', MODIFY_ID = '%s' where " % (data,modifyName,modifyId)
        for i in oids:
            sql += "oid = '"+ i +"' or "
        sql = sql[0:-3]
        # print sql
        # sql = "SELECT STOP_WORD_DICT_URL, TEMPORARY_WORD_DICT_URL, NEW_DICT_NUM, MAX_DICT_NUM,CLASS_WORD_DICT_URL FROM DATAPREDICT_OPINION_DICT;"
        # sql = "SELECT OID, NEW_WORD, WORD_FREQUENCY, WORD_PART, EDIT_WORD, DICTIONARY_FLAG, IGNORE_FLAG, DEAL_FLAG, WORD_USED_FLAG FROM DATAPREDICT_OPINION_INFO;"
        cursor.execute(sql)
    except Exception as ex:
        logging.error("updateDealFlag Failed: %s\t" % (ex))

'''
初始化词典表的数据
'''
def initDictData (cursor,obj) :
    time = str(datetime.datetime.now().strftime('%Y-%m-%d'))
    stop_word_dict_url = str("F:\\esoft\\xinfang\\other\\stopword.txt")
    temporary_word_dict_url = str("F:\\esoft\\xinfang\\other\\dict\\userDict.txt")
    class_word_dict_url = str("F:\\esoft\\xinfang\\other\\dict\\" + str(obj.get("id")) + ".txt")
    has_child_flag = 1
    if(obj.get("leaf")):
        has_child_flag = 0
    sql = "INSERT INTO DATAPREDICT_OPINION_DICT (OID, STOP_WORD_DICT_URL, TEMPORARY_WORD_DICT_URL, CREATE_DATE, CREATOR_NAME, CREATOR_ID, MODIFY_DATE, MODIFY_NAME, MODIFY_ID, CLASS_WORD_DICT_URL, HAS_CHILD_FLAG, PARENT_ID, CODE, DICT_NAME) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" % (str(obj.get("oid")),stop_word_dict_url,temporary_word_dict_url,time,'','',time,'','',class_word_dict_url,has_child_flag,str(obj.get("parentId")),str(obj.get("id")),str(obj.get("text")))

    import re
    regex = re.compile(r'\\')
    sql = regex.sub(r"\\\\", sql)
    # cursor.execute(sql)


'''
初始化词到词典
'''
def addDictFile (cursor) :
    try:
        sql = "SELECT PARENT_CODE, WORD, WORD_FREQUENCY FROM DATAPREDICT_OPINION_WORD_BASIC;"
        cursor.execute(sql)
        rows = cursor.fetchall()
        if rows != []:
            petition_df = pd.DataFrame(rows)
            petition_df.columns = ['PARENT_CODE', 'WORD', 'WORD_FREQUENCY']
            return petition_df
        else :
            return None
    except Exception as ex:
        logging.error("updateDealFlag Failed: %s\t" % (ex))

'''
删除暂存词的列表
'''
def deleteWord(cursor):
    logging.info("Start deleteWord  newWord!")
    try:
        sql = "DELETE FROM DATAPREDICT_OPINION_WORD"
        cursor.execute(sql)
    except Exception as ex:
        logging.error("deleteWord Failed : %s\t" % (ex))
    logging.info("End deleteWord  newWord!")

'''
删除暂存词的列表
'''
def deleteOldWord(cursor):
    logging.info("Start deleteOldWord  oldWord!")
    try:
        sql = "DELETE FROM DATAPREDICT_OPINION_OLD_WORD"
        cursor.execute(sql)
    except Exception as ex:
        logging.error("deleteWord Failed : %s\t" % (ex))
    logging.info("End deleteOldWord  oldWord!")


def deleteOtherWord(cursor):
    logging.info("Start deleteOldWord  oldWord!")
    try:
        sql = "DELETE FROM DATAPREDICT_OPINION_INFO WHERE DICTIONARY_FLAG = '0' AND DEAL_FLAG = '1'"
        cursor.execute(sql)
    except Exception as ex:
        logging.error("deleteWord Failed : %s\t" % (ex))
    logging.info("End deleteOldWord  oldWord!")


def UPDD(cursor):
    logging.info("Start deleteOldWord  oldWord!")
    try:
        sql = "DELETE FROM DATAPREDICT_OPINION_INFO WHERE DICTIONARY_FLAG = '0' AND DEAL_FLAG = '1'"
        cursor.execute(sql)
    except Exception as ex:
        logging.error("deleteWord Failed : %s\t" % (ex))
    logging.info("End deleteOldWord  oldWord!")

'''
获取要从头开始执行
'''
def getDealFlag(cursor):
    logging.info("Start getDealFlag!")
    try:
        sql = "select * from DATAPREDICT_OPINION_SETTING where DEAL_FLAG != '1'"
        cursor.execute(sql)
        rows = cursor.fetchall()
        if rows != []:
            return False
        else :
            return True
    except Exception as ex:
        logging.error("getDealFlag Failed : %s\t" % (ex))
        return False
    logging.info("End getDealFlag  !")

'''
新的一次，删除所有数据
'''

def deleteSettAll(cursor):
    logging.info("Start deleteSettAll!")
    try:
        sql = "delete from  DATAPREDICT_OPINION_SETTING"
        cursor.execute(sql)
    except Exception as ex:
        logging.error("deleteSettAll Failed : %s\t" % (ex))
        return None
    logging.info("End deleteSettAll  !")

'''
获取要从头开始执行
'''
def saveDealUuid(cursor,dealUuid,allRound,onceNum):
    try:
        logging.info("Start saveDealUuid!")
        if(len(dealUuid) > 0 ) :
           dealUuidS = ','.join(dealUuid)
        else :
            dealUuidS = ''

        # 创建人信息
        path = os.path.realpath(sys.argv[0])
        createName = getConf("common", "createName", path)
        createId = getConf("common", "createId", path)
        uuid = shortuuid.ShortUUID().random(length=20)
        data = str(time.strftime('%Y-%m-%d', time.localtime(time.time())))

        sql = "INSERT INTO DATAPREDICT_OPINION_SETTING (OID, ALL_ROUND,CURR_ROUND, CURR_STRIP,ONCE_STRIP,CREATE_DATE, CREATOR_NAME, CREATOR_ID, MODIFY_DATE, DEAL_UUID,SAVE_WORD_FLAG,DEAL_FLAG) VALUES ('%s', %s, %s,%s,%s,'%s', '%s', '%s', '%s', '%s',%s,%s) "  % (uuid,allRound,0,0,onceNum,data,createName,createId,data,dealUuidS,0,0)
        cursor.execute(sql)
        logging.info("End saveDealUuid!")
    except Exception as ex:
        logging.error("saveDealUuid Failed : %s\t" % (ex))



'''
获取当前运行信息
'''
def getNowMess (cursor) :
    logging.info("Start getNowMess!")
    try:
        sql = "SELECT  ALL_ROUND, CURR_ROUND, CURR_STRIP ,ONCE_STRIP FROM DATAPREDICT_OPINION_SETTING where MODIFY_DATE = (select max(MODIFY_DATE) from  DATAPREDICT_OPINION_SETTING);"
        cursor.execute(sql)
        rows = cursor.fetchall()
        if rows != []:
            petition_df = pd.DataFrame(rows)
            petition_df.columns = ['ALL_ROUND', 'CURR_ROUND', 'CURR_STRIP','ONCE_STRIP']
            logging.info("End getNowMess!")
            return petition_df
        else :
            logging.info("End getNowMess!")
            return None
    except Exception as ex:
        logging.error("getNowMess Failed: %s\t" % (ex))



'''
更新轮数
'''
def updateSettCR (cursor,CURR_ROUND) :
    logging.info("Start updateSettCR!")
    try:
        path = os.path.realpath(sys.argv[0])
        modifyName = getConf("common", "modifyName", path)
        modifyId = getConf("common", "modifyId", path)
        data = str(time.strftime('%Y-%m-%d', time.localtime(time.time())))

        sql = "update DATAPREDICT_OPINION_SETTING set CURR_ROUND = %s,CURR_STRIP = 0,MODIFY_DATE = '%s', MODIFY_NAME = '%s', MODIFY_ID = '%s' where deal_flag != 1" % (CURR_ROUND,data,modifyName,modifyId)
        # print sql
        cursor.execute(sql)
    except Exception as ex:
        logging.error("updateSettCR Failed: %s\t" % (ex))
    logging.info("End updateSettCR!")


# '''
# 更新轮数
# '''
# def updateSettCR (cursor,CURR_STRIP) :
#     logging.info("Start updateSettCR!")
#     try:
#         path = os.path.realpath(sys.argv[0])
#         modifyName = getConf("common", "modifyName", path)
#         modifyId = getConf("common", "modifyId", path)
#         data = str(time.strftime('%Y-%m-%d', time.localtime(time.time())))
#         sql = "update DATAPREDICT_OPINION_SETTING set CURR_STRIP = %s,MODIFY_DATE = '%s', MODIFY_NAME = '%s', MODIFY_ID = '%s') where deal_flag != 1;" % (CURR_STRIP,data,modifyName,modifyId)
#         cursor.execute(sql)
#     except Exception as ex:
#         logging.error("updateSettCR Failed: %s\t" % (ex))
#     logging.info("End updateSettCR!")



'''
更新条数
'''
def updateSettCT (cursor,CURR_ROUND) :
    logging.info("Start updateSettCT!")
    try:
        path = os.path.realpath(sys.argv[0])
        modifyName = getConf("common", "modifyName", path)
        modifyId = getConf("common", "modifyId", path)
        data = str(time.strftime('%Y-%m-%d', time.localtime(time.time())))

        sql = "update DATAPREDICT_OPINION_SETTING set CURR_STRIP = %s,MODIFY_DATE = '%s', MODIFY_NAME = '%s', MODIFY_ID = '%s';" % (CURR_ROUND,data,modifyName,modifyId)
        cursor.execute(sql)
    except Exception as ex:
        logging.error("updateSettCT Failed: %s\t" % (ex))
    logging.info("End updateSettCT!")




def getdealUuid (cursor) :
    logging.info("Start getdealUuid!")
    try:
        sql = "SELECT  DEAL_UUID FROM DATAPREDICT_OPINION_SETTING where MODIFY_DATE = (select max(MODIFY_DATE) from  DATAPREDICT_OPINION_SETTING);"
        cursor.execute(sql)
        rows = cursor.fetchall()
        if rows != []:
            petition_df = pd.DataFrame(rows)
            petition_df.columns = ['DEAL_UUID']
            return petition_df
        else :
            return None
    except Exception as ex:
        logging.error("getdealUuid Failed: %s\t" % (ex))
    logging.info("End getdealUuid!")



'''
更新最后状态
'''

def updateSettDealFlag(cursor):
    logging.info("Start updateSettDealFlag!")
    try:
        path = os.path.realpath(sys.argv[0])
        modifyName = getConf("common", "modifyName", path)
        modifyId = getConf("common", "modifyId", path)
        data = str(time.strftime('%Y-%m-%d', time.localtime(time.time())))

        sql = "update DATAPREDICT_OPINION_SETTING set deal_flag = 1,MODIFY_DATE = '%s', MODIFY_NAME = '%s', MODIFY_ID = '%s' where deal_flag != 1" % ( data, modifyName, modifyId)
        print sql
        cursor.execute(sql)
    except Exception as ex:
        logging.error("updateSettDealFlag Failed: %s\t" % (ex))
    logging.info("End updateSettDealFlag!")



