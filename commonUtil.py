# -*- coding: utf-8 -*-
'''
@author: liuzd
@time: 2017/8/12 16:01
@Function: 连接db2
'''

import ibm_db_dbi
import ConfigParser
import os
import logging
import sys
def db():  # 取配置文件参数
    cf = ConfigParser.ConfigParser()
    # cf.read(os.path.dirname(os.path.realpath(__file__)) + "//fpConf.ini")
    cf.read("fpConf.ini")
    # -get(section,option) 得到section中option的值，返回为string类型
    database = cf.get("db", "database")
    hostname = cf.get("db", "hostname")
    port = cf.getint("db", "port")
    user = cf.get("db", "user")
    passwd = cf.get("db", "passwd")
    modelCycle = cf.getint("model", "modelCycle")  # 存储模型的时间（天）
    supportCity = cf.getint("model", "supportCity")  # 上海市支持度
    supportArea = cf.getint("model", "supportArea")  # 区支持度
    supportWei = cf.getint("model", "supportWei")  # 委办支持度
    return database, hostname, port, user, passwd, modelCycle, supportCity, supportArea, supportWei


def connection(database, hostname, port, user, passwd):
    try:
        conn = ibm_db_dbi.connect(
            "DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;UID=%s;PWD=%s" % (database, hostname, port, user, passwd),
            "",
            "")
        conn.set_autocommit(True)
        cursor = conn.cursor()
        return cursor, conn
    except Exception as e:
       logging = getLog()
       logging.error("连接数据库失败")
       logging.error(e)


def getConf(confO,confT,fileNme) :
    cf = ConfigParser.ConfigParser()
    # path = os.path.dirname(os.path.dirname(os.path.abspath(fileNme)))

    cf.read("fpConf.ini")
    conf = cf.get(confO,confT)
    return conf
def getConfInt(confO,confT,fileNme) :
    cf = ConfigParser.ConfigParser()
    # path = os.path.dirname(os.path.dirname(os.path.abspath(fileNme)))

    cf.read("fpConf.ini")
    conf = cf.getint(confO,confT)
    return conf

def getConfig(confO,confT,fileNme) :
    cf = ConfigParser.ConfigParser()
    path = os.path.dirname(os.path.dirname(os.path.abspath(fileNme)))

    cf.read(path + "\\fpConf.ini")
    conf = cf.get(confO,confT)
    return conf

# def getLog():
#     path = os.path.realpath(sys.argv[0])
#     logging.basicConfig(level=logging.DEBUG,
#                         format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
#                         datefmt='%a, %d %b %Y %H:%M:%S',
#                         filename=getConf("log", "logUrl", path),
#                         filemode='a')
#     return logging

import logging # 生成一个日志对象
import logging.handlers
def getLog():
    path = os.path.realpath(sys.argv[0])
    # getConf("log", uur, path)
    # logging.basicConfig(level=logging.DEBUG,
    #                     format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
    #                     datefmt='%a, %d %b %Y %H:%M:%S',
    #                     filename=getConf("log", "logUrl", path),
    #                     filemode='a')

    logger = logging.getLogger()
    if not logger.handlers:
        # 生成一个Handler。logging支持许多Handler，例如FileHandler, SocketHandler, SMTPHandler等，

    # 我由于要写文件就使用了FileHandler。
        # LOG_FILE是一个全局变量，它就是一个文件名，如：'crawl.log'
        LOG_FILE = getConf("log", "logUrl", path)
        # hdlr = logging.FileHandler(LOG_FILE)
        # 生成一个格式器，用于规范日志的输出格式。如果没有这行代码，那么缺省的
        # 格式就是："%(message)s"。也就是写日志时，信息是什么日志中就是什么，
        # 没有日期，没有信息级别等信息。logging支持许多种替换值，详细请看
        # Formatter的文档说明。这里有三项：时间，信息级别，日志信息
        formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
        # 将格式器设置到处理器上

        # logger.addHandler(hdlr)
        hdlr = logging.handlers.TimedRotatingFileHandler(LOG_FILE, when='H', interval=1, backupCount=10)
        hdlr.setFormatter(formatter)
        # 再创建一个handler，用于输出到控制台
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)


        # 将处理器加到日志对象上
        logger.addHandler(hdlr)
        logger.addHandler(ch)
        # 设置日志信息输出的级别。logging提供多种级别的日志信息，如：NOTSET,
        # DEBUG, INFO, WARNING, ERROR, CRITICAL等。每个级别都对应一个数值。
        # 如果不执行此句，缺省为30(WARNING)。可以执行：logging.getLevelName
        # (logger.getEffectiveLevel())来查看缺省的日志级别。日志对象对于不同
        # 的级别信息提供不同的函数进行输出，如：info(), error(), debug()等。当
        # 写入日志时，小于指定级别的信息将被忽略。因此为了输出想要的日志级别一定
        # 要设置好此参数。这里我设为NOTSET（值为0），也就是想输出所有信息
        logger.setLevel(logging.DEBUG)
    return logger



'''
加密解密方法

'''
def enc(s):
    a=bytearray(s.decode('utf-8').encode('gbk'))
    ret = ""
    for aa in a:
        ret += str(hex(aa)[2:])
    return ret

'''
加密解密方法

'''

def dec(s):
    bits = ""
    try:
        for i in xrange(0, len(s), 2):
            q = chr(int(s[i:i + 2], 16))
            bits += q
        return bits.decode("GBK")
    except:
        return ""
