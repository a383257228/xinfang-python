# -*- coding: utf-8 -*-
'''
@ author: jh
@ time: 2017/8/17
@ Function: 主函数
'''
from __future__ import division
import numpy as np
import time
import datetime
from baseData import *
from sklearn.ensemble import RandomForestRegressor
from commonUtil import getLog  # 从文件路径为connectionDB2的文件中导入所有的函数
import logging

# 获取日志对象
logging = getLog()


# 时间处理方法1：
def time_function(df_data):
    # 时间处理：
    day = df_data.Petition_Date.apply(lambda x: str(x)[0:10])
    df_data["day"] = pd.DataFrame(day)
    t = df_data.day.apply(lambda x: datetime.date(int(x[0:4]), int(x[5:7]), int(x[8:10])))
    df_data['day_time'] = pd.DataFrame(t)
    return df_data


# 时间处理方法2：
def time_function2(df_data):
    # 时间处理：
    t = df_data.yearmonth.apply(lambda x: datetime.date(x // 100, x % 100, 1))
    df_data['day_time'] = pd.DataFrame(t)
    return df_data


# 时间窗口创建
def window(star_day, end_day, f_time, pre_time):
    time_window = []
    start_year = star_day // 100
    s = star_day % 100
    end = datetime.date(start_year, 1, 21)
    j = 0
    while end < end_day:
        year = start_year + (s + j) // 12
        if (s + j) % 12 == 0:
            month = 12
            start = datetime.date(year - 1, month, 21)
        else:
            month = (s + j) % 12
            start = datetime.date(year, month, 21)

        window = 12 * f_time  # 2:160; 6:112
        year = start_year + (s + j + window) // 12
        if (s + j + window) % 12 == 0:
            month = 12
            m = datetime.date(year - 1, month, 21)
        else:
            month = (s + j + window) % 12
            m = datetime.date(year, month, 21)
        time = window + pre_time  # 预测下一个月的信访量
        year = start_year + (s + j + time) // 12
        if (s + j + time) % 12 == 0:
            month = 12
            end = datetime.date(year - 1, month, 21)
        else:
            month = (s + j + time) % 12
            end = datetime.date(year, month, 21)
        # print j, start, m, end
        time_window.append([start, m, end, j])
        j += 1
    return time_window


# 提取特征方法1（预测2~6个月）
def handle_feature2(feature, month, i):
    col = ['Issue_Region_Code4']
    if i in [1, 2, 3, 6, 9, 12, 15, 18, 24, 30, 36, 48, 60, 72]:
        # ---------------------求数量：
        num = month.groupby(col, as_index=False)['num'].agg({'month' + str(i) + '_num': np.sum})
        feature = pd.merge(feature, num, how='left')
    if i in [6, 12, 24, 36, 48, 60, 72]:
        # ---------------------求均值方差
        month_mean = month.groupby(col, as_index=False)['num'].agg({'month' + str(i) + '_mean': np.mean})
        month_std = month.groupby(col, as_index=False)['num'].agg({'month' + str(i) + '_std': np.std})
        feature = pd.merge(feature, month_mean, how='left')
        feature = pd.merge(feature, month_std, how='left')
    return feature


# 提取特征方法2（预测第1个月）
def handle_feature(feature, month, i, Source, Petition_Type, object_class, Issue_Type, Deal_Type, Reality):
    col = ['Issue_Region_Code4']
    if i in [1, 2, 3, 6, 9, 12, 15, 18, 24]:
        # ---------------------求数量：
        num = month.groupby(col, as_index=False)['Issue_Region_Code4'].agg({'month' + str(i) + '_num': np.size})
        feature = pd.merge(feature, num, how='left')
    if i in [6, 12, 24, 36, 48, 60, 72]:
        # ---------------------求均值方差
        everymonth_num = month.groupby(['Issue_Region_Code4', 'year_month'], as_index=False)['Issue_Region_Code4'].agg(
            {'everymonth_num': np.size})
        month_mean = everymonth_num.groupby(col, as_index=False)['everymonth_num'].agg(
            {'month' + str(i) + '_mean': np.mean})
        month_std = everymonth_num.groupby(col, as_index=False)['everymonth_num'].agg(
            {'month' + str(i) + '_std': np.std})
        feature = pd.merge(feature, month_mean, how='left')
        feature = pd.merge(feature, month_std, how='left')
    if i in [1, 6, 12, 24, 36, 72]:
        # ---------------------来源
        for t in Source.Source_Code4:
            type = month[month.Source_Code4 == t]
            name = 'month' + str(i) + '_Source_' + str(t) + '_num'
            Source_num = type.groupby(['Issue_Region_Code4'], as_index=False)['Issue_Region_Code4'].agg({name: np.size})
            feature = pd.merge(feature, Source_num, how='left')
        # ---------------------来访方式
        for t in Petition_Type.Petition_Type_Code:
            type = month[month.Petition_Type_Code == t]
            name = 'month' + str(i) + '_Petition_Type_' + str(t) + '_num'
            Petition_Type_num = type.groupby(['Issue_Region_Code4'], as_index=False)['Issue_Region_Code4'].agg(
                {name: np.size})
            feature = pd.merge(feature, Petition_Type_num, how='left')
        # ---------------------行政级别
        for t in object_class.Object_Class_Code:
            type = month[month.Object_Class_Code == t]
            name = 'month' + str(i) + '_object_class_' + str(t) + '_num'
            object_class_num = type.groupby(['Issue_Region_Code4'], as_index=False)['Issue_Region_Code4'].agg(
                {name: np.size})
            feature = pd.merge(feature, object_class_num, how='left')

    if i in [12, 24, 48]:  # , 48, 72]:
        # ---------------------问题类别：
        for t in Issue_Type.Issue_Type_Code:
            type = month[month.Issue_Type_Code == t]
            name = 'month' + str(i) + '_Issue_Type_' + str(t) + '_num'
            Issue_Type_num = type.groupby(['Issue_Region_Code4'], as_index=False)['Issue_Region_Code4'].agg(
                {name: np.size})
            feature = pd.merge(feature, Issue_Type_num, how='left')
        # 处置方式：初步核实--谈话函询--了结--暂存--交办--转办
        for t in Deal_Type.Deal_Type_Code:
            real = month[month.Deal_Type_Code == t]
            name = 'month' + str(i) + '_Deal_Type_' + str(t) + '_num'
            Deal_Type_num = real.groupby(['Issue_Region_Code4'], as_index=False)['Issue_Region_Code4'].agg(
                {name: np.size})
            feature = pd.merge(feature, Deal_Type_num, how='left')
        # ---------------------问题种类与问题属实程度有哪些：
        for t in Reality.Reality_Code:
            real = month[month.Reality_Code == t]
            name = 'month' + str(i) + '_Reality_' + str(t) + '_num'
            Reality_num = real.groupby(['Issue_Region_Code4'], as_index=False)['Issue_Region_Code4'].agg(
                {name: np.size})
            feature = pd.merge(feature, Reality_num, how='left')
    return feature


def feature_function(time_window, region_data, near_12_month, region):
    # 特征因素筛选
    Source1 = near_12_month.groupby(['Source_Code4'], as_index=False)['Issue_Region_Code4'].agg({'num': np.size})
    Source = Source1[Source1.num > 30]
    Petition_Type1 = near_12_month.groupby(['Petition_Type_Code'], as_index=False)['Issue_Region_Code4'].agg(
        {'num': np.size})
    Petition_Type = Petition_Type1[Petition_Type1.num > 30]
    object_class1 = near_12_month.groupby(['Object_Class_Code'], as_index=False)['Issue_Region_Code4'].agg(
        {'num': np.size})
    object_class = object_class1[object_class1.num > 30]
    Issue_Type1 = near_12_month.groupby(['Issue_Type_Code'], as_index=False)['Issue_Region_Code4'].agg({'num': np.size})
    Issue_Type = Issue_Type1[Issue_Type1.num > 30]
    Deal_Type1 = near_12_month.groupby(['Deal_Type_Code'], as_index=False)['Issue_Region_Code4'].agg({'num': np.size})
    Deal_Type = Deal_Type1[Deal_Type1.num > 30]
    Reality1 = near_12_month.groupby(['Reality_Code'], as_index=False)['Issue_Region_Code4'].agg({'num': np.size})
    Reality = Reality1[Reality1.num > 30]
    # 循环窗口提取特征
    train_finally = pd.DataFrame()  # train
    for w in time_window:
        print (w[0], w[1], w[2], w[3])
        qu_data_feature = region_data[(region_data.day_time >= w[0]) & (region_data.day_time < w[1])]
        # 创建特征集开始的第一列
        feature = pd.DataFrame({'Issue_Region_Code4': region})
        # 循环最近xx月，提取特征
        for i in [1, 2, 3, 6, 9, 12, 15, 18, 24, 30, 36, 48, 60, 72]:
            now = w[1]
            ii = i
            if ii < 72:
                while ii >= 1:
                    now = (now.replace(day=1) - datetime.timedelta(1)).replace(day=21)
                    ii -= 1
                # 最近xxx个月
                month = qu_data_feature[qu_data_feature.day_time >= now]
            else:
                month = qu_data_feature
            # 特征提取调用方法
            feature = handle_feature(feature, month, i, Source, Petition_Type, object_class, Issue_Type, Deal_Type,
                                     Reality)
            # print feature
        # 去年同月信访量
        lastyear_samemonth = region_data[(region_data.day_time >= datetime.date(w[1].year - 1, w[1].month, 21)) & (
        region_data.day_time < datetime.date(w[2].year - 1, w[2].month, 21))]
        samemonth_num = lastyear_samemonth.groupby(['Issue_Region_Code4'], as_index=False)['Issue_Region_Code4'].agg(
            {'samemonth_num': np.size})
        feature = pd.merge(feature, samemonth_num, how='left')
        feature = feature.fillna(value=0)  # 循环到最后---是test
        if w[3] < time_window[-1][3]:
            train_finally = train_finally.append(feature)  # 循环到最后---是train
    feature.index = range(len(feature))
    train_finally.index = range(len(train_finally))
    return train_finally, feature


def label_function(time_window, region_data, region):
    train_finally = pd.DataFrame()
    for w in time_window:
        print (w[0], w[1], w[2], w[3])
        lab = region_data[(region_data.day_time >= w[1]) & (region_data.day_time < w[2])]
        feature = pd.DataFrame({'Issue_Region_Code4': region})
        lab_num = lab.groupby(['Issue_Region_Code4'], as_index=False)['Issue_Region_Code4'].agg({'lab_num': np.size})
        feature = pd.merge(feature, lab_num, how='outer')
        feature = feature.fillna(value=0)
        if w[3] < time_window[-1][3]:
            train_finally = train_finally.append(feature)  # 循环到最后---是train
    train_finally.index = range(len(train_finally))
    return train_finally


def feature_function2(time_window, region_data, region):
    # 循环窗口提取特征
    train_finally = pd.DataFrame()  # train
    for w in time_window:
        print (w[0], w[1], w[2], w[3])
        qu_data_feature = region_data[(region_data.day_time >= w[0]) & (region_data.day_time < w[1])]
        # 创建特征集开始的第一列
        feature = pd.DataFrame({'Issue_Region_Code4': region})
        # 循环最近xx月，提取特征
        for i in [1, 2, 3, 6, 9, 12, 15, 18, 24, 30, 36, 48, 60, 72]:
            now = w[1]
            ii = i
            if ii < 72:
                while ii >= 1:
                    now = (now.replace(day=1) - datetime.timedelta(1)).replace(day=21)
                    ii -= 1
                # 最近xxx个月
                month = qu_data_feature[qu_data_feature.day_time >= now]
            else:
                month = qu_data_feature
            # 特征提取调用方法
            feature = handle_feature2(feature, month, i)
        # 去年同月信访量
        lastyear_samemonth = region_data[(region_data.day_time >= datetime.date(w[1].year - 1, w[1].month, 21)) & (
        region_data.day_time < datetime.date(w[2].year - 1, w[2].month, 21))]
        samemonth_num = lastyear_samemonth.groupby(['Issue_Region_Code4'], as_index=False)['num'].agg(
            {'samemonth_num': np.sum})
        feature = pd.merge(feature, samemonth_num, how='left')
        # 预测月的上一个月与去年上一个月信访量比较
        lastmonth = (w[1].replace(day=1) - datetime.timedelta(1)).replace(day=21)  # 是最近一个月的
        lastyear_lastmonth = region_data[
            (region_data.day_time >= datetime.date(lastmonth.year - 1, lastmonth.month, 21)) & (
            region_data.day_time < datetime.date(w[1].year - 1, w[1].month, 21))]
        lastyear_lastmonth_num = lastyear_lastmonth.groupby(['Issue_Region_Code4'], as_index=False)[
            'Issue_Region_Code4'].agg({'lastmonth_num': np.size})
        feature = pd.merge(feature, lastyear_lastmonth_num, how='left')
        feature['lastmonth_bi'] = feature['month1_num'] / feature['lastmonth_num']
        feature = feature.drop(['lastmonth_num'], axis=1)
        feature = feature.fillna(value=0)  # 循环到最后---是test
        if w[3] < time_window[-1][3]:
            train_finally = train_finally.append(feature)  # 循环到最后---是train
    feature.index = range(len(feature))
    train_finally.index = range(len(train_finally))
    return train_finally, feature


def label_function2(time_window, region_data, region):
    train_finally = pd.DataFrame()
    for w in time_window:
        print (w[0], w[1], w[2], w[3])
        lab = region_data[(region_data.day_time >= w[1]) & (region_data.day_time < w[2])]
        feature = pd.DataFrame({'Issue_Region_Code4': region})
        lab_num = lab.groupby(['Issue_Region_Code4'], as_index=False)['num'].agg({'lab_num': np.sum})
        feature = pd.merge(feature, lab_num, how='outer')
        feature = feature.fillna(value=0)
        if w[3] < time_window[-1][3]:
            train_finally = train_finally.append(feature)  # 循环到最后---是train
    train_finally.index = range(len(train_finally))
    return train_finally


def model_function(train, test):
    pred = test[['Issue_Region_Code4']]
    test_x = test.drop(['Issue_Region_Code4'], axis=1)
    train_x = train.drop(['Issue_Region_Code4', 'lab_num'], axis=1)
    train_y = train.lab_num
    model = RandomForestRegressor(n_estimators=8, max_depth=8, max_features=0.7, n_jobs=-1, random_state=1024)
    model.fit(train_x, train_y)
    pred['num'] = model.predict(test_x)
    pred['num'] = pred.num.apply(lambda x: int(x))
    # names = train_x.columns
    # a = pd.DataFrame(sorted(zip(map(lambda x: round(x, 4), model.feature_importances_), names), reverse=True))
    # a.columns = ['im', 'col']
    return pred


# 提取特征，训练模型，返回预测结果（ 先预测区，再预测派驻，最后预测上海市）
def predict_function(region_all, region_data_all, now_y_m_start, start_day, end_day):
    finally_pred = pd.DataFrame()  # 最终所有的预测结果
    for i in range(3):  # i=0时把区的数据输进去--预测，i=1时把委的数据输进去--预测，i=2时把上海市的数据输进去--预测
        # 窗口构建
        time_window = window(start_day, end_day, f_time=6, pre_time=1)
        # 数据
        region = region_all[i]
        region_data = region_data_all[i]
        now_y_m = now_y_m_start
        # 距预测最近的12个月
        near_12_month = region_data[(region_data.yearmonth >= (now_y_m - 100)) & (region_data.yearmonth < now_y_m)]
        # 提取特征
        try:
            train_f, test_f = feature_function(time_window, region_data, near_12_month, region)
        except Exception as e:
            logging.error("feature_function: %s\t" % (e))
        try:
            train_l = label_function(time_window, region_data, region)
        except Exception as e:
            logging.error("label_function: %s\t" % (e))
        # if (len(train_f) == len(train_l)) & (train_f.shape[1] == test_f.shape[1]):
        train = train_f.join(train_l['lab_num'])  # 训练集
        test = test_f  # 预测集

        # 预测下1个月（共预测6个月）
        try:
            pred = model_function(train, test)
        except Exception as e:
            logging.error("model_function: %s\t" % (e))
        pred['yearmonth'] = now_y_m  # 填上日期（预测第1个月）
        # 预测接下来2——6个月 （把预测的数据（test_val)加进去  日期max_time）
        rd = region_data.groupby(['Issue_Region_Code4', 'yearmonth'], as_index=False)['Issue_Region_Code4'].agg(
            {'num': np.size})
        # 接下来用于提取特征的数据
        region_data2 = rd[['Issue_Region_Code4', 'num', 'yearmonth']]
        region_data2 = region_data2.append(pred)
        if (i == 2):  # 上海是特殊，需要求和（预测结果） 
            shs_num = pred.num.sum()
            pred = pd.DataFrame({'Issue_Region_Code4': ['3100'], 'num': [shs_num], 'yearmonth': [now_y_m]})
        pred_all = pred  # 把每次的预测结果添加到一起
        for j in range(1, 6):
            print (train_f.shape, train_l.shape, test_f.shape, train.shape)
            print ('--------------------------------j:', j)
            # 下一个要预测的日期是什么（年月）比如：201704
            if (now_y_m % 100) < 12:
                now_y_m = now_y_m + 1
            else:
                now_y_m = ((now_y_m + 100) // 100) * 100 + 1
            end_day1 = datetime.date(now_y_m // 100, now_y_m % 100, 21)  # 需要预测月的结束日期
            # 窗口构建
            time_window = window(start_day, end_day1, f_time=6, pre_time=1)
            # 时间处理
            region_data2 = time_function2(region_data2)
            # 构建特征工程，训练模型
            train_f, test_f = feature_function2(time_window, region_data2, region)
            train_l = label_function2(time_window, region_data2, region)
            train = train_f.join(train_l['lab_num'])  # (80, 30) (80, 2) (16, 30)  <---(64, 259) (64, 2) (16, 259)
            test = test_f
            # 预测
            pred = model_function(train, test)
            pred['yearmonth'] = now_y_m  # 填上日期（预测第1个月）201706
            # 接下来用于提取特征的数据
            region_data2 = region_data2.append(pred)
            # 当预测上海市时需要对预测结果求和
            if (i == 2):
                shs_num = pred.num.sum()
                pred = pd.DataFrame({'Issue_Region_Code4': ['3100'], 'num': [shs_num], 'yearmonth': [now_y_m]})
            pred_all = pred_all.append(pred)  # 把每次的预测结果添加到一起
        finally_pred = finally_pred.append(pred_all)  # 将区，委，市的预测结果放一起
    return finally_pred


def project_function(qws_data, org):
    logging.info("start project_function.")
    # 时间处理
    qws_data = time_function(qws_data)
    qws_data['yearmonth'] = qws_data.year_month.apply(lambda x: int(x[0:4] + x[5:7]))
    # 历史数据最大日期201706
    max_time = qws_data['Petition_Date'].max()
    df_maxtime = qws_data[qws_data.Petition_Date == max_time]
    now_y_m = df_maxtime.iloc[0, -1]  # 现在是获取数据库中的最大时间，以后要改成获取当前系统所在时间
    start_day = now_y_m - 1000  # 开始
    # start_day=201102
    print now_y_m
    end_day = datetime.date(now_y_m // 100, now_y_m % 100, 21)  # 需要预测月的结束日期

    # 上海市
    shs_data = qws_data[qws_data.qws == 1]
    shs_data.index = range(len(shs_data))
    shs = list(shs_data.drop_duplicates(['Issue_Region_Code4'])['Issue_Region_Code4'])
    # 派驻
    wei_data = qws_data[qws_data.qws == 3]
    wei_data.index = range(len(wei_data))
    wei = list(wei_data.drop_duplicates(['Issue_Region_Code4'])['Issue_Region_Code4'])
    # 16个区
    qu_data = qws_data[qws_data.qws == 2]
    qu_data.index = range(len(qu_data))
    qu = list(qu_data.drop_duplicates(['Issue_Region_Code4'])['Issue_Region_Code4'])
    # 把区，派驻，上海市分别放入列表里
    region_all = [qu, wei, shs]
    region_data_all = [qu_data, wei_data, shs_data]
    # 调用函数提取特征，训练模型，返回预测结果（ 先预测区，再预测派驻，最后预测上海市）
    finally_pred = predict_function(region_all, region_data_all, now_y_m, start_day, end_day)
    if len(finally_pred) != 0:
        # 对预测结果做最终处理，关联组织机构名称，并对时间进行处理
        time = list(set(finally_pred['yearmonth']))
        org_data = pd.DataFrame()
        for t in time:
            d = finally_pred[finally_pred.yearmonth == t]
            org['yearmonth'] = t
            od = pd.merge(org, d, how='left')
            od.index = range(len(od))
            od = od.fillna(value=0)
            if (t % 100) == 1:
                od['time'] = datetime.date((t - 100) // 100, 12, 21)
            else:
                od['time'] = datetime.date(t // 100, t % 100 - 1, 21)
            org_data = org_data.append(od)
        # 预测结果
        fin_pred = org_data[['Issue_Region_Code4', 'Issue_Region_Name', 'num', 'qws', 'time']]
        fin_pred.index = range(len(fin_pred))
        return fin_pred
    else:
        logging.warning("project_function:\t The forecast result is null.")
        return None


def mainLD(cursor, conn):
    try:
        logging = getLog()
        logging.info("start mainLD.")
        start = time.clock()
        # 从数据库中取出数据
        qws_data = selectData(cursor)
        org = selectOrganization(cursor)
        if (qws_data is not None) & (org is not None):
            if (len(qws_data) != 0) & (len(org) != 0):
                # 构建特征工程，得到区，委，市未来6个月的预测结果
                pred = project_function(qws_data, org)
                if pred is not None:
                    today = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 当前日期
                    # 将得到的结果插入数据库中
                    insert_function(cursor, conn, pred, today)
                    logging.info("未来六个月的信访分布预测存储完毕.")
            else:
                logging.warning("mainLD:\t Data is empty.")
        else:
            logging.warning("mainLD:\t select error.")
        end = time.clock()  # 运行结束时间
        logging.info("The program runs successfully. takes time:%0.2fs" % (end - start))
        print(u'\n未来六个月的信访分布预测存储完毕，用时：%0.2f秒' % (end - start))  # 打印运行总时间
    except Exception as e:
        logging.error("The program runs fail. %s\t" % (e))
