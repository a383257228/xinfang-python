# -*- coding: utf-8 -*-
import time
from lettersDistribution.predict import *  # 信访分布
from forest.makeForest import mainFR
from Opinion.initOpinion import *
from fpm.partitionAnalysisQL import *
from fpm.partitionAnalysisQLCompletion import * 
from apscheduler.schedulers.blocking import BlockingScheduler
from commonUtil import *  # 从文件路径为connectionDB2的文件中导入所有的函数
logging = getLog()

# 李杨
def mainOP():
    #    print(2)
    start = time.clock()
    database, hostname, port, user, passwd, modelCycle, supportCity, supportArea, supportWei = db()
    cursor, conn = connection(database, hostname, port, user, passwd)  # 连接数据库
    logging.info( '民意分析--成功连接数据库')
    status = init(conn, cursor)
    # if (status == "success"):
    #     cursor.close()  # 关闭连接
    #     conn.commit()
    #     conn.close()
    # else:
    #     cursor.close()  # 关闭连接
    #     conn.close()
    cursor.close()  # 关闭连接
    conn.close()
    # initDictFile(cursor)

    logging.info('民意分析--执行结束关闭数据库连接')
    end = time.clock()
    logging.info('民意分析--完成，用时：%0.2f秒' % (end - start))  # 打印运行总时间


# 姜慧
def mainLD1():
    start = time.clock()
    database, hostname, port, user, passwd, modelCycle, supportCity, supportArea, supportWei = db()
    cursor, conn = connection(database, hostname, port, user, passwd)  # 连接数据库
    logging.info('信访件分布--连接数据库')
    mainLD(cursor, conn)
    cursor.close()  # 关闭连接
    conn.close()
    logging.info( '信访件分布--执行结束关闭数据库连接')
    end = time.clock()
    logging.info('信访件分布--完成，用时：%0.2f秒' % (end - start))  # 打印运行总时间


# 高宁蔚
def mainFR1():
    start = time.clock()
    database, hostname, port, user, passwd, modelCycle, supportCity, supportArea, supportWei = db()
    cursor, conn = connection(database, hostname, port, user, passwd)  # 连接数据库
    logging.info('聚焦分析--成功连接数据库')
    mainFR(conn, cursor)
    cursor.close()  # 关闭连接
    conn.close()
    logging.info( '聚焦分析--执行结束关闭数据库连接')
    end = time.clock()
    logging.info('聚焦分析--完成，用时：%0.2f秒' % (end - start))  # 打印运行总时间


# 振东
def mainQLQD():
    start = time.clock()
    database, hostname, port, user, passwd, modelCycle, supportCity, supportArea, supportWei = db()
    cursor, conn = connection(database, hostname, port, user, passwd)  # 连接数据库
    logging.info('问题类别分布--成功连接数据库')
    # mainQL(cursor, modelCycle, conn, supportCity, supportArea, supportWei)
    # mainQD(cursor, modelCycle, conn, supportCity, supportArea, supportWei)
    # mainCB(cursor, modelCycle, conn, supportCity, supportArea, supportWei)
    mainQL(cursor, modelCycle, conn, supportCity, supportArea, supportWei)
    mainQLC(cursor, modelCycle, conn, supportCity, supportArea, supportWei)
    cursor.close()  # 关闭连接
    conn.close()
    logging.info( '问题类别分布--执行结束关闭数据库连接')
    end = time.clock()
    logging.info('问题类别分布--完成，用时：%0.2f秒' % (end - start))  # 打印运行总时间

if __name__ == '__main__':
    sched = BlockingScheduler()
    #mainOP()
    #mainLD1()
    #mainFR1()
    #mainQLQD()
    sched.add_job(mainOP, 'interval', seconds=30)
    sched.add_job(mainLD1, 'interval', seconds=30)
    sched.add_job(mainFR1, 'interval', seconds=30)
    sched.add_job(mainQLQD, 'interval', seconds=30)
    #sched.add_job(mainOP, 'cron',month='1-12', day='21', hour='21',minute='0',second='0')
    #sched.add_job(mainLD1, 'cron',month='1-12', day='21', hour='21',minute='0',second='0')
    #sched.add_job(mainFR1, 'cron',month='1-12', day='21', hour='21',minute='0',second='0')
    #sched.add_job(mainQLQD, 'cron',month='1-12', day='21', hour='21',minute='0',second='0')
    sched.start()
