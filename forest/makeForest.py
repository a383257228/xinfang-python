# -*- coding: utf-8 -*-
# from __future__ import print_function
from commonUtil import *
import gc
# from commonUtil import dec
import pandas as pd
import itertools
from itertools import combinations
import thread
import time
import re
import jieba.posseg
from gensim import corpora
from gensim.similarities import Similarity
import copy
import shortuuid
import psutil
# import logging
#import sys
#reload(sys)
#sys.setdefaultencoding('utf8')

max_mem=95#内存上限
min_similarity=0.35#最小相似度
max_similarity=1.00
max_similar_num=3#匹配最接近的2个petition
address_len=4#地址取前2个字
user_dict_path='dict/gword.txt'#用户词库
nomi_list = ['n', 'j', 'eng', 'ns', 'vn', 'nr', 'b', 'l', 'nt', 'a', 'f', 'x', 'v', 'i']
# s_file=open("forest/success.txt", 'a')

#刘晨等科级干部以及科委领导金龙军班子成员
accused_amb=[r.decode('utf8') for r in['有关','相关','个别','某','xx','XX','长$','^学','人员','部分',
                                       '些','领导','单位','部门','警察','同志','人员','医生','护士','客人',
                                       '(.){2,4}部','^校']]#不明确

accused_del=[r.decode('utf8') for r in['(无)','不详','^上海市$','等(.){0,2}人','等']]
accused_split=[r.decode('utf8') for r in['、','，','；','以及','及','/','\\']]#分隔符

col_weight=[3,1,5,2,1]#unit,home,petition,accuser,predict
logging = getLog()

def nameFilter(acc,un):#某
    ac=acc
    try:
        #处理括号
        if re.search('（|）'.decode('utf8'), ac):
            if not re.search('（(女|男|音|\s)）|（）'.decode('utf8'), ac):
                if re.search('（(.*)长|某）'.decode('utf8'), ac):
                    ac = ac.replace('（'.decode('utf8'), '').replace('）'.decode('utf8'), '')
                else:
                    ac = re.sub('（(.*)）'.decode('utf8'), '', ac)
                    ac = ac.replace('（'.decode('utf8'), '').replace('）'.decode('utf8'), '')
        for aa in accused_amb:
            if re.search(aa, ac):
                if len(un) > 1:
                    return un + ac
                else:
                    return ''
        return ac
    except:
        # f_file.write("61")
        return ''

def nameCheck(ac,un=''):
    try:
        result=ac
        for de in accused_del:
            # result=result.replace(de,'')
            result = re.sub(de, '', result)
        if len(result)<2:return ''
        for sp in accused_split:
            result=result.replace(sp,';')
        result=[nameFilter(r,un) for r in result.split(';')]
        return result
    except:
        # f_file.write("76 " + ac + " " + un + "\n")
        return ''

def sqlUpdate(a1,a2,col,str,w):
    sql="MERGE INTO ZZ_ACCUSED_NEXUS USING " \
        "( SELECT * FROM TABLE( VALUES('%s','%s')))AS g(m,n) " \
        "ON (ZZ_ACCUSED_NEXUS.ACCUSED_1 = g.m AND ZZ_ACCUSED_NEXUS.ACCUSED_2=g.n AND NEXUS=0)" \
        "WHEN MATCHED THEN UPDATE SET (DISTANCE,%s) = (DISTANCE+%d,(CASE " \
        "WHEN COALESCE(length(%s),0)>5000 THEN '-' " \
        "WHEN %s = '-' THEN '-' " \
        "ELSE (CONCAT(COALESCE(%s,''),'%s')) END))" \
        "WHEN NOT MATCHED THEN INSERT(ID,ACCUSED_1,ACCUSED_2,NEXUS,DISTANCE,%s) VALUES ('%s',g.m,g.n,0,%d,'%s')" % (
        a1,a2,col,w,col,col,col,str,col,makeId(),w,str
    )
    sql="MERGE INTO TF_ACCUSED_NEXUS USING " \
        "( SELECT * FROM TABLE( VALUES('%s','%s')))AS g(m,n) " \
        "ON (TF_ACCUSED_NEXUS.ACCUSED_1 = g.m AND TF_ACCUSED_NEXUS.ACCUSED_2=g.n AND NEXUS=0)" \
        "WHEN MATCHED THEN UPDATE SET (DISTANCE,%s) = (DISTANCE+%d,(CASE " \
        "WHEN COALESCE(length(%s),0)>5000 THEN '-' " \
        "WHEN %s = '-' THEN '-' " \
        "ELSE (CONCAT(COALESCE(%s,''),'%s')) END))" \
        "WHEN NOT MATCHED THEN INSERT(ID,ACCUSED_1,ACCUSED_2,NEXUS,DISTANCE,%s) VALUES ('%s',g.m,g.n,0,%d,'%s')" % (
        a1,a2,col,w,col,col,col,str,col,makeId(),w,str
    )
    return sql

def cs(conn,cursor,sql):
    try:
        # print sql
        cursor.execute(sql)
        conn.commit()
        # s_file.write("-- " + sql.encode('utf8') + "\n")
        logging.info('sql committed ')
    except:
        # f_file.write("~~ " + sql.encode('utf8') + "\n")
        logging.error('sql failed : '+sql.encode('utf8'))


# def commitSql():
#     for k in ppdict:
#         v = ppdict[k]
#         sql = "INSERT INTO TF_PETITION_NEXUS(ID,OID_1,OID_2,SIMILARITY) " \
#               "VALUES ('%s','%s','%s','%f')" \
#               % (makeId(), k[0], k[1], v * 100)

def checkUnit(unit,region):
    try:

        if unit== '上海市'.decode('utf8'):
            return ''
        # if unit[0] == '区'.decode('utf8'):
            # print 'aa'
        if re.search('^(区|校|局)'.decode('utf8'), unit):
            return region+unit[1:]
        else:
            return unit
    except:
        # f_file.write("129 "+unit+" "+region+"\n")
        return ''

def getAccusedInfo(cursor):
    logging.debug('querying petition accused info')

    sql="SELECT ACCUSED_NAME,ACCUSED_HOME_PLACE,ACCUSED_WORK_UNIT,ACCUSED_REGION_NAME FROM ZZ_PETITION_ACCUSED_INFO"
    sql="SELECT ACCUSED_NAME,ACCUSED_HOME_PLACE,ACCUSED_WORK_UNIT,ACCUSED_REGION_NAME FROM PETITION_ACCUSED_INFO"
    cursor.execute(sql)
    rows = cursor.fetchall()
    df_info = pd.DataFrame(rows)
    df_info.columns = ['accused', 'home', 'unit','region']

    df_info['home'] = df_info['home'].map(dec)
    logging.debug('proceccing accused name & unit')
    df_info['unit'] = df_info['unit'].map(dec)
    df_info['accused'] = df_info['accused'].map(dec)
    df_info['unit']=map(lambda x, y: checkUnit(x, y), df_info['unit'], df_info['region'])
    df_info['accused'] = map(lambda x, y: nameCheck(x, y), df_info['accused'], df_info['unit'])#df_info['accused'].map(dec).map(nameCheck)
    return df_info[['accused', 'home', 'unit']]

def collectUnit(df_unit,conn,cursor):
    logging.debug('collecting work units')
    group_unit = df_unit.groupby('unit')
    for unit in group_unit:
        if len(unit[0]) < 2:
            continue
        accused_unit = list(unit[1]['accused'])
        temp=list(itertools.chain.from_iterable(accused_unit))
        accused_combs_0 = list(combinations(temp, 2))
        for accused_comb in accused_combs_0:
            accused_1 = accused_comb[0]
            accused_2 = accused_comb[1]
            if len(accused_1) < 2 or len(accused_2) < 2:
                continue
            if accused_1 == accused_2:
                continue
            if accused_1 > accused_2:
                accused_1, accused_2 = accused_2, accused_1
            # print sqlUpdate(accused_1, accused_2, 'WORK_UNIT', unit[0] + ';', col_weight[0])
            cs(conn,cursor,sqlUpdate(accused_1, accused_2, 'WORK_UNIT', unit[0] + ';', col_weight[0]))

def prefix(home):
    try:
        if len(home)>address_len:
            return home[0:address_len]
        else:
            return home
    except:
            return ''

def collectHome(df_home,conn,cursor):
    # zzdict={}
    logging.debug('collecting home addresses')
    df_home['home'] = df_home['home'].map(prefix)
    group_home = df_home.groupby('home')
    for home in group_home:
        if len(home[0]) < 1:
            continue
        accused_home = list(home[1]['accused'])
        temp = list(itertools.chain.from_iterable(accused_home))
        accused_combs_1 = list(combinations(temp, 2))
        for accused_comb in accused_combs_1:
            accused_1 = accused_comb[0]
            accused_2 = accused_comb[1]
            if len(accused_1) < 2 or len(accused_2) < 2:
                continue
            if accused_1 == accused_2:
                continue
            if accused_1 > accused_2:
                accused_1, accused_2 = accused_2, accused_1
            # print sqlUpdate(accused_1, accused_2, 'HOME_PLACE', home[0] + ';', col_weight[1])
            cs(conn, cursor, sqlUpdate(accused_1, accused_2, 'HOME_PLACE', home[0] + ';',col_weight[1]))

def getPetitionYears(cursor):
    sql="SELECT UNIQUE SUBSTR(PETITION_DATE,1,4) FROM PETITION_BASIC_INFO"
    cursor.execute(sql)
    rows = cursor.fetchall()
    return ['----']+[r[0] for r in rows]

def getPetitionInfo(cursor,ys,ye):

    sql = "SELECT ZZ_PETITION_BASIC_INFO.OID,ZZ_PETITION_BASIC_INFO.FIRST_ACCUSER,ZZ_PETITION_BASIC_INFO.FIRST_ACCUSED,ZZ_PETITION_BASIC_INFO.OTHER_ACCUSEDS," \
          "ZZ_PETITION_ISSUE_INFO.ISSUE_REGION_NAME,ZZ_PETITION_ISSUE_INFO.KEY_WORD_CONTENT,ZZ_PETITION_ISSUE_INFO.ISSUE_CONTENT,SUBSTR(ZZ_PETITION_BASIC_INFO.PETITION_DATE,1,4) " \
          "FROM ZZ_PETITION_BASIC_INFO FULL OUTER JOIN ZZ_PETITION_ISSUE_INFO ON ZZ_PETITION_BASIC_INFO.OID=ZZ_PETITION_ISSUE_INFO.PETITION_BASIC_INFO_OID " \
          "WHERE SUBSTR(ZZ_PETITION_BASIC_INFO.PETITION_DATE,1,4) IN('%s','%s')"%(ys,ye)
    sql = "SELECT PETITION_BASIC_INFO.OID,PETITION_BASIC_INFO.FIRST_ACCUSER,PETITION_BASIC_INFO.FIRST_ACCUSED,PETITION_BASIC_INFO.OTHER_ACCUSEDS," \
          "PETITION_ISSUE_INFO.ISSUE_REGION_NAME,PETITION_ISSUE_INFO.KEY_WORD_CONTENT,PETITION_ISSUE_INFO.ISSUE_CONTENT,SUBSTR(PETITION_BASIC_INFO.PETITION_DATE,1,4)" \
          "FROM PETITION_BASIC_INFO FULL OUTER JOIN PETITION_ISSUE_INFO ON PETITION_BASIC_INFO.OID=PETITION_ISSUE_INFO.PETITION_BASIC_INFO_OID " \
          "WHERE SUBSTR(PETITION_BASIC_INFO.PETITION_DATE,1,4) IN('%s','%s')"%(ys,ye)

    cursor.execute(sql)
    rows = cursor.fetchall()
    df_report_o = pd.DataFrame(rows)#[0:100]
    df_report_o.columns = ['id', 'accuser', 'accused1', 'accuseds2', 'region', 'keywords', 'content','year']
    df_report_o['accuser']=df_report_o['accuser'].map(dec)#.map(nameCheck)
    df_report_o['accused1'] = df_report_o['accused1'].map(dec).map(nameCheck)
    df_report_o['accused'] = df_report_o['accuseds2'].map(dec).map(nameCheck)#.map(f)
    df_report_o['content'] = df_report_o['content'].map(dec)
    df_report_o['accused']=map(lambda x, y: g(x, y), df_report_o['accused1'], df_report_o['accused'])
    # df_report_o['title'] = df_report_o['title'].map(dec)
    # print df_report_o[['id','accuser','accused','keywords','content','year']]
    return df_report_o[['id', 'accuser', 'accused', 'keywords', 'content', 'year']]
    # return df_report_o[['id','accuser','accused1','accuseds2','keywords','content','year']]

def f(x):#'a;b;c'=>['a','b','c']
    if x=='':
        return []
    if isinstance(x,basestring):
        return [nameCheck(y) for y in x.split(';')]
        # return x.split(';')
    else:
        return []

def g(x,y):#['a','b'],'c'=>['a','b','c']
    try:
        return x+y
    except:
        return []

def threadAccuser(df_report,conn,cursor,flag,ys):
    for i in range(0,len(df_report)):
        oldrow=df_report[i:i + 1]
        aid=list(oldrow['id'])[0]
        try:
            small_accuseds = sum(oldrow['accused'].tolist(), [])
            # print small_accuseds
        except:
            continue
        accused_combs_2=list(combinations(small_accuseds, 2))
        #
        if list(oldrow['year'])[0]!=ys:
            for accused_comb in accused_combs_2:
                try:
                    accused_1=accused_comb[0]
                    accused_2=accused_comb[1]
                    if len(accused_1)<2 or len(accused_2)<2:
                        continue
                except:
                    continue
                if accused_1>accused_2:
                    accused_1,accused_2=accused_2,accused_1
                # print sqlUpdate(accused_1, accused_2, 'PETITION_NO', aid + ';', col_weight[2])
                cs(conn, cursor, sqlUpdate(accused_1, accused_2, 'PETITION_NO',aid + ';', col_weight[2]))

    flag[0]-=1
    logging.debug(str(flag[0])+' threads left '+ys)

def collectPetition(df_report_o,conn,cursor,ys):
    logging.debug('collecting petition '+ys)

    # len_report = len(df_report_o)
    # df_report1 = df_report_o[0:len_report / 3]
    # df_report2 = df_report_o[len_report / 3 * 1:len_report / 3 * 2]
    # df_report3 = df_report_o[len_report / 3 * 2:]
    flag_o = [3]
    try:
        e=1/0
        logging.debug('segging accuser data, 3 threads started')
        # thread.start_new_thread(threadAccuser, (df_report1, df_accused, zzdict, flag_o))
        # thread.start_new_thread(threadAccuser, (df_report2, df_accused, zzdict, flag_o))
        # thread.start_new_thread(threadAccuser, (df_report3, df_accused, zzdict, flag_o))
    except:
        logging.warning('thread exception, now retrying '+ys)
        threadAccuser(df_report_o,conn,cursor, flag_o,ys)
        flag_o[0] = 0
    while flag_o[0] > 0:
        pass
    #print df_accused[['accused','accuser','id']]
    # print df_accused


def h(l1,l2,l3):#[1,4,7],[2,5,8],[3,6,9]=>[(1,2,3),(4,5,6),(7,8,9)]
    l=[]

    for i in range(0,len(l1)):
        for ii in l1[i]:
            l.append((ii,l2[i],l3[i]))
    return l

def collectAccuser(df_accuser,conn,cursor,ys):#ys
    logging.debug('collecting accuser '+ys)
    group_accuser = df_accuser.groupby('accuser')
    logging.debug('accuser group complete '+ys)

    for accuser in group_accuser:
        if len(accuser[0]) < 2:
            continue
        accused_id = h(list(accuser[1]['accused']), list(accuser[1]['id']),list(accuser[1]['year']))  # [(被举报人1,id1),(被举报人2,id2)...]
        # print accused_id[0]
        accused_combs_3 = list(combinations(accused_id, 2))
        for accused_comb in accused_combs_3:
            if accused_comb[0][2]==accused_comb[1][2]==ys:
                continue
            if len(accused_comb[0][0]) < 2 or len(accused_comb[1][0]) < 2:
                continue
            if accused_comb[0][1] == accused_comb[1][1] or accused_comb[0][0] == accused_comb[1][0]:
                continue
            accused_1 = accused_comb[0][0]
            accused_2 = accused_comb[1][0]
            if accused_1 > accused_2:
                accused_1, accused_2 = accused_2, accused_1
            # print sqlUpdate(accused_1, accused_2, 'ACCUSER', accuser[0] + ';', col_weight[3])
            cs(conn, cursor, sqlUpdate(accused_1, accused_2, 'ACCUSER', accuser[0] + ';', col_weight[3]))

def addDict(cursor,f,cName,tName,d):
    sql="SELECT DISTINCT "+cName+" FROM "+tName
    cursor.execute(sql)
    rows = list(cursor.fetchall())
    if d:
        for row in rows:
            row=dec(row)
            try:
                if len(row[0]) == 0: continue
                f.write(row[0].encode('utf8') + "\n")
            except:
                1
    else:
        for row in rows:
            try:
                if len(row[0]) == 0: continue
                f.write(row[0].encode('utf8') + "\n")
            except:
                1

def makeDict(cursor):
    logging.debug('making user dictionary')
    f = open(user_dict_path, 'a')
    addDict(cursor,f,"REGION_NAME", "PETITION_ACCUSED_INFO",False)
    addDict(cursor,f,"ACCUSED_NAME", "PETITION_ACCUSED_INFO",True)
    addDict(cursor,f,"ACCUSED_HOME_PLACE", "PETITION_ACCUSED_INFO",True)
    addDict(cursor,f,"ACCUSED_WORK_UNIT", "PETITION_ACCUSED_INFO",True)
    addDict(cursor,f,"ACCUSED_REGION_NAME", "PETITION_ACCUSED_INFO",True)
    f.close()
    try:
        jieba.load_userdict(user_dict_path)
        logging.debug('successfully loaded user dictionary')
    except:
        logging.error('failed to load user dictionary')

def wordSeg(line):#'Ihaveanapple'=>'I have an apple'
    try:
        line=re.sub('[\s+]','',line)
    except:
        logging.error('wordSeg exception: Ignoring line '+line)
        line=""
    if (line==""):
        return ''
    else:
        seg = jieba.posseg.cut(line)
        l=''
        for i in seg:
            if i.flag in nomi_list:
                if len(i.word) > 1:
                    l+= ' '+i.word
        return l

def getSeggedContent(df_report_o):
    logging.debug('segging petition content')
    df_content_o = df_report_o[['id', 'content']]
    df_content_o['content'] = df_content_o['content'].map(wordSeg)
    return df_content_o

def getSimilarity(df_content_o):
    logging.debug('preparing docSim')
    raw_documents = list(df_content_o['content'])
    corpora_documents = []
    for item_text in raw_documents:
        item_str = item_text.split(' ')
        corpora_documents.append(item_str)
    dictionary = corpora.Dictionary(corpora_documents)
    corpus = [dictionary.doc2bow(text) for text in corpora_documents]
    nf=len(set(itertools.chain.from_iterable(corpora_documents)))+1
    similarity = Similarity('-Similarity-index', corpus, num_features=nf)#!!!!!!!!!!!!!!!!!!!!!
    similarity.num_best = max_similar_num
    return similarity,dictionary

def getNearest(id,sim_model,dictionary,df_content_o):
    result=[]
    content=list(df_content_o[df_content_o['id']==id]['content'])[0]
    test_corpus=dictionary.doc2bow(content.split(' '))
    try:
        for i in sim_model[test_corpus]:#错误
            if i[1]>min_similarity and i[1]<max_similarity:
                try:
                    result.append((df_content_o['id'][i[0]], i[1]))
                except:
                    logging.error('exception processing petition id: '+id)
    except:
        logging.error('failed to get nearest petitions, id:' +id)
    return result

def threadPrediction(df_accused2,flag,similarity,ppdict,dictionary,df_content_o,conn,cursor,ys):#start
    # print '--------------',ys
    temp_similarity = copy.deepcopy(similarity)
    for id in df_accused2['id']:
        try:
            acy1=df_accused2[df_accused2['id']==id]
            accuseds,y1=list(acy1['accused'])[0],list(acy1['year'])[0]
        except:
            continue

        nearests=getNearest(id,temp_similarity,dictionary,df_content_o)
        # print nearests
        for nearest_sim in nearests:
            nearest=nearest_sim[0]
            sim=nearest_sim[1]
            try:
                acy2 = df_accused2[df_accused2['id'] == nearest]
                n_accuseds, y2 = list(acy2['accused'])[0], list(acy2['year'])[0]
            except:
                continue
            if y1==y2==ys:
                continue
            # print y1 + '        ' + y2 + '        ' + ys
            if nearest == id:
                continue

            if nearest>id:
                nearest,id=id,nearest
            ppdict[(nearest,id)]=sim
            for accused in accuseds:
                for n_accused in n_accuseds:  # 21
                    accused_1=accused
                    accused_2=n_accused
                    try:
                        if len(accused_1)<2 or len(accused_2)<2:
                            continue
                    except:
                        continue
                    if accused_1==accused_2:
                        continue
                    if accused_1>accused_2:
                        if y1==y2:
                            continue
                        accused_1,accused_2=accused_2,accused_1
                    # print sqlUpdate(accused_1, accused_2, 'PETITION_NO_PREDICTED', toStr2((nearest,id)) + ';', col_weight[4])
                    cs(conn, cursor, sqlUpdate(accused_1, accused_2, 'PETITION_NO_PREDICTED', toStr2((nearest,id)) + ';', col_weight[4]))


    flag[0]-=1
    logging.debug(str(flag[0])+' threads left '+ys)
    return ppdict

def collectPrediction(df_report_o,conn,cursor,ys):
    logging.debug('collecting prediction '+ys)
    df_content_o = getSeggedContent(df_report_o)
    ppdict = {}
    df_accused2_o = df_report_o[['id', 'accused','year']]
    # len_accused2 = len(df_accused2_o)
    # df_accused21 = df_accused2_o[0:len_accused2 / 3]
    # df_accused22 = df_accused2_o[len_accused2 / 3 * 1:len_accused2 / 3 * 2]
    # df_accused23 = df_accused2_o[len_accused2 / 3 * 2:]
    flag_o = [3]
    try:
        similarity,dictionary=getSimilarity(df_content_o)
        logging.debug('successfully built similarity '+ys)
    except:
        logging.error('failed to get Similarity '+ys)
        return ppdict
    try:
        e=1/0
        logging.debug('segging petition data, 3 threads started '+ys)
        # thread.start_new_thread(threadPrediction, (df_accused22, zzdict, flag_o,similarity,ppdict,dictionary,df_content_o))
        # thread.start_new_thread(threadPrediction, (df_accused23, zzdict, flag_o,similarity,ppdict,dictionary,df_content_o))
        # thread.start_new_thread(threadPrediction, (df_accused21, zzdict, flag_o,similarity,ppdict,dictionary,df_content_o))
    except:
        logging.warning('thread exception, now retrying '+ys)
        ppdict=threadPrediction(df_accused2_o,flag_o,similarity,ppdict,dictionary,df_content_o,conn,cursor,ys)
        flag_o[0] = 0
    while flag_o[0] > 0:
        pass
    return ppdict

def prepareTables(a,b): #
    conn,cursor=a,b #
    logging.debug('preparing tables')
    try:
        logging.debug('clearing table TF_PETITION_NEXUS')
        sql = "ALTER TABLE TF_PETITION_NEXUS activate NOT LOGGED initially WITH EMPTY TABLE"
        cursor.execute(sql)
        conn.commit()
    except:
        try:
            sql = "DROP TABLE TF_PETITION_NEXUS"
            cursor.execute(sql)
            conn.commit()
        except:
            logging.debug('creating table TF_PETITION_NEXUS')
            sql = "CREATE TABLE TF_PETITION_NEXUS(ID CHAR(20) NOT NULL,OID_1 CHAR(20) NOT NULL,OID_2 CHAR(20) NOT NULL,SIMILARITY DECIMAL,PRIMARY KEY (ID))"
            cursor.execute(sql)
            conn.commit()
    try:
        logging.debug('clearing table TF_ACCUSED_NEXUS1')
        sql = "DELETE FROM ZZ_ACCUSED_NEXUS WHERE NEXUS=0"
        sql = "DELETE FROM TF_ACCUSED_NEXUS WHERE NEXUS=0"
        cursor.execute(sql)
        conn.commit()
    except:
        try:
            logging.error('clear failed, table will be droped ')
            sql = "DROP TABLE ZZ_ACCUSED_NEXUS"
            sql = "DROP TABLE TF_ACCUSED_NEXUS"
            cursor.execute(sql)
            conn.commit()
        except:
            logging.debug('creating table TF_ACCUSED_NEXUS1')
            sql = "CREATE TABLE ZZ_ACCUSED_NEXUS(ID CHAR(20) NOT NULL,ACCUSED_1 VARCHAR(100) NOT NULL,ACCUSED_2 VARCHAR(100) NOT NULL,NEXUS VARCHAR(100)," \
                  "DISTANCE BIGINT,WORK_UNIT VARCHAR(6000),HOME_PLACE VARCHAR(6000),PETITION_NO VARCHAR(6000),ACCUSER VARCHAR(6000)," \
                  "PETITION_NO_PREDICTED VARCHAR(6000),PRIMARY KEY (ID))"
            sql = "CREATE TABLE TF_ACCUSED_NEXUS(ID CHAR(20) NOT NULL,ACCUSED_1 VARCHAR(100) NOT NULL,ACCUSED_2 VARCHAR(100) NOT NULL,NEXUS VARCHAR(100)," \
                  "DISTANCE BIGINT,WORK_UNIT VARCHAR(6000),HOME_PLACE VARCHAR(6000),PETITION_NO VARCHAR(6000),ACCUSER VARCHAR(6000)," \
                  "PETITION_NO_PREDICTED VARCHAR(6000),PRIMARY KEY (ID))"
            cursor.execute(sql)
            conn.commit()

def updateTables(conn,cursor):
    try:
        sql = "DELETE FROM ZZ_ACCUSED_NEXUS WHERE NEXUS=1"
        sql="DELETE FROM TF_ACCUSED_NEXUS WHERE NEXUS=1"
        cursor.execute(sql)
        conn.commit()
        logging.debug('old data deleted')
        try:
            sql = "UPDATE ZZ_ACCUSED_NEXUS SET NEXUS=1"
            sql = "UPDATE TF_ACCUSED_NEXUS SET NEXUS=1"
            cursor.execute(sql)
            conn.commit()
            logging.debug('update data complete')
        except:
            logging.error('update data failed')
    except:
        logging.error('deleting old data failed')

def nexus(x):#[[xxx,yyy],[zzz]]=>[2,1]
    result=[]
    for xx in x:
        result.append(len(xx))
    return result

def dist(x):#[0,1,2,3,4]=>10(先随便写一个)
    result=0
    for xx in x:
        result+=xx
    return result

def toStr(x):#[xxx,yyy]=>xxx;yyy;
    result=''
    for xx in x:
        try:
            result+=unicode(str(xx),"utf-8")+';'
        except:
            result+=xx+';'
    return result

def toStr2(x):#(aaa,bbb)=>'aaa%%%bbb'
    # result=[]
    # for xx in x:
    #     result.append(xx[0]+"%%%"+xx[1])
    #     if len(result)>135:
    #         return result
    return x[0]+"%%%"+x[1]

def makeId():
    return shortuuid.ShortUUID().random(length=20)

def sinkPredict(conn,cursor,ppdict):
    logging.debug('inserting into tables')
    for k in ppdict:
        v = ppdict[k]
        sql = "INSERT INTO TF_PETITION_NEXUS(ID,OID_1,OID_2,SIMILARITY) " \
              "VALUES ('%s','%s','%s','%f')" \
              % (makeId(), k[0], k[1], v * 100)
        try:
            cursor.execute(sql)
            conn.commit()
            # print sql
            logging.info('-sql committed ')
        except:
            logging.error('-sql failed')
    # for k in zzdict:
    #     v = zzdict[k]
    #     sql = "INSERT INTO TF_ACCUSED_NEXUS(ID,ACCUSED_1,ACCUSED_2,NEXUS,DISTANCE,WORK_UNIT,HOME_PLACE,PETITION_NO,ACCUSER,PETITION_NO_PREDICTED) " \
    #           "VALUES ('%s','%s','%s','%s','%d','%s','%s','%s','%s','%s')" \
    #           % (
    #           makeId(), k[0], k[1], toStr(nexus(v)), dist(nexus(v)), toStr(v[0]), toStr(v[1]), toStr(v[2]), toStr(v[3]),
    #           toStr(toStr2(v[4])))
    #     try:
    #         #e=1/0
    #         cursor.execute(sql)
    #         conn.commit()
    #         print sql
    #         logging.info('sql committed ')
    #     except:
    #         logging.error('sql failed, trying to recomit')
    #         try:
    #             sql = "INSERT INTO TF_ACCUSED_NEXUS(ID,ACCUSED_1,ACCUSED_2,NEXUS,DISTANCE,WORK_UNIT,HOME_PLACE,PETITION_NO,ACCUSER,PETITION_NO_PREDICTED) " \
    #                   "VALUES ('%s','%s','%s','%s','%d','-','-','-','-','-')" \
    #                   % (
    #                       makeId(), k[0], k[1], toStr(nexus(v)), dist(nexus(v))
    #                       )
    #             cursor.execute(sql)
    #             conn.commit()
    #             print sql
    #             logging.info('successfully recommitted')
    #         except:
    #             logging.error('failed to recommit')

def memCheck(run):
    while 1:
        time.sleep(5)
        mp = psutil.virtual_memory().percent
        if mp>max_mem:
            err='out of memory: %f'%(mp)
            logging.error(err)
            run[0]=0
            sys.exit()

# def print_time(run,keep):
#     count = 0
#     while 1:
#         time.sleep(1)
#         count+=1
#         print count
#         if count>keep:
#             logging.error('kkkkk')
#             run[0]=0
#             sys.exit()



def mainThread(conn,cursor,run):
    try:
        prepareTables(conn,cursor)##############
    except:
        logging.error('prepare tables failed')
        run[0] = 0

    pp_dict={}##############
    try:
        df_info = getAccusedInfo(cursor)##############
    except:
        logging.error('get accused info failed')
        run[0] = 0
    ##############
    try:
        collectUnit(df_info[['accused', 'unit']],conn,cursor)
    except:
        logging.error('collect unit failed')

    try:
        collectHome(df_info[['accused','home']],conn,cursor)
    except:
        logging.error('collect home failed')

    try:
        del df_info
        gc.collect()
        logging.debug('del df_info complete')
    except:
        logging.error('del df_info failed')

    try:
        makeDict(cursor)
    except:
        e=1

    try:
        years = getPetitionYears(cursor)
    except:
        logging.error('get Petition years failed!')
        run[0] = 0

    for y in range(len(years)-1):
        try:
            ys=years[y]
            ye=years[y+1]
            yi=ys+'--'+ye
        except:
            logging.error('yi error')
            continue
        try:
            logging.debug('querying petition info ' + yi)
            df_report_o = getPetitionInfo(cursor, ys, ye)
        except:
            logging.warning('no petition record ' + yi)
            continue

        try:
            collectPetition(df_report_o, conn, cursor,ys)
        except:
            logging.error('collect petition failed, '+yi)

        try:
            collectAccuser(df_report_o[['accused','accuser','year','id']], conn, cursor,ys)
            logging.debug('collect accuser complete, '+yi)
        except:
            logging.error('collect accuser failed, '+yi)

        #
        try:
            pp_dict = collectPrediction(df_report_o, conn, cursor,ys)
        except:
            logging.error('predict failed, '+yi)
        #
        try:
            del df_report_o
            gc.collect()
            logging.debug('del df_report_o complete, '+yi)
        except:
            logging.error('del df_report_o failed, '+yi)

        try:
            sinkPredict(conn, cursor, pp_dict)
            logging.debug('sink petition nexus complete, '+yi)
        except:
            logging.error('sink petition nexus failed, '+yi)
        try:
            for root, dirs, files in os.walk("./"):
                for fi in files:
                    if fi[0] == "-":
                        os.remove(fi)
            logging.debug('forest complete, '+yi)
        except:
            logging.error('failed to delete temp file, '+yi)
    logging.debug('all years complete, setting run to 0')
    run[0] = 0

def mainFR(conn,cursor):
    run=[1]
    thread.start_new_thread(memCheck, (run,))
    thread.start_new_thread(mainThread, (conn,cursor,run))
    # thread.start_new_thread(print_time, (run,20))
    while run[0] == 1:
        pass
    logging.debug('updating tables')
    if os.path.exists(user_dict_path):
        os.remove(user_dict_path)

    updateTables(conn, cursor)
    logging.debug('forest finished')
# def quit()



