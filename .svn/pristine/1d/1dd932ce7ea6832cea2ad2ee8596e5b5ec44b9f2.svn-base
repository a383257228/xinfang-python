# -*- coding: utf-8 -*-
from commonUtil import *  # 从文件路径为connectionDB2的文件中导入所有的函数
from fpm.partitionAnalysisQL import *  #
from fpm.partitionAnalysisQD import *
from fpm.partitionAnalysisQLCompletion import *  #
from fpm.top5 import *
from fpm.top5Completion import *  # 从文件路径为top5的文件中导入所有的函数
from fpm.pChange import *
if __name__ == '__main__':
    start = time.clock()
    database, hostname, port, user, passwd, modelCycle, supportCity, supportArea, supportWei = db()
    cursor, conn = connection(database, hostname, port, user, passwd)  # 连接数据库
    # sql = "delete from DATAPREDICT_OPINION_INFO where CREATE_DATE is not null"
    # cursor.execute(sql)

    # cursor.close()
    # conn.rollback()
    # conn.commit()
    # conn.close()

    print u'成功连接数据库'
    # mainLD(cursor, conn)  # 信访分布
    # mainQD(cursor, modelCycle, conn, supportCity, supportArea, supportWei)  # 问题类别与处理方式之间的隐藏关系
    # mainQL(cursor, modelCycle, conn, supportCity, supportArea, supportWei)  # 问题类别与行政级别之间的隐藏关系
    # mainQLC(cursor, modelCycle, conn, supportCity, supportArea, supportWei)  # 问题类别与行政级别之间的隐藏关系
    # mainQLCB(cursor, conn)

    mainQD(cursor, modelCycle, conn, supportCity, supportArea, supportWei)  # 问题类别与处理方式之间的隐藏关系
    mainQL(cursor, modelCycle, conn, supportCity, supportArea, supportWei)  # 问题类别与行政级别之间的隐藏关系
    mainQLC(cursor, modelCycle, conn, supportCity, supportArea, supportWei)  # 问题类别与行政级别之间的隐藏关系
    mainQC(cursor, conn, modelCycle)  # 问题类别变化情况
    mainTop5(cursor, conn, modelCycle)  # 问题top5
    mainTop5C(cursor, conn, modelCycle)  # 问题top5

    # 保存新词到词典里
    # saveDict(cursor)
    # 分词
    # participle(cursor)
    # init(cursor)
    # status = init(conn,cursor)
    # if(status == "success") :
    #     cursor.close()  # 关闭连接
    #     conn.commit()
    #     conn.close()
    # else :
    cursor.close()  # 关闭连接
    conn.close()
    # initDictFile(cursor)
    # cursor.close()  # 关闭连接
    # conn.commit()
    # conn.close()
    print '执行结束关闭db2数据库连接'
    end = time.clock()
    print(u'\n执行完毕，总用时：%0.2f秒' % (end - start))
