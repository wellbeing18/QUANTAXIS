# coding: utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2018 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import datetime

import numpy
import pandas as pd
from pandas import DataFrame

from QUANTAXIS.QAUtil import (DATABASE, QA_Setting, QA_util_date_stamp,
                              QA_util_date_valid, QA_util_dict_remove_key,
                              QA_util_log_info, QA_util_code_tolist, QA_util_date_str2int, QA_util_date_int2str,
                              QA_util_sql_mongo_sort_DESCENDING,
                              QA_util_time_stamp, QA_util_to_json_from_pandas,
                              trade_date_sse)
from QUANTAXIS.QAData.financial_mean import financial_dict

"""
按要求从数据库取数据，并转换成numpy结构

2018-07-30 修改 增加batch_size  可以做到8MB/S-30mb/s的传输速度

"""


def QA_fetch_stock_day(code, start, end, format='numpy', frequence='day', collections=DATABASE.stock_day):
    '获取股票日线'
    start = str(start)[0:10]
    end = str(end)[0:10]
    #code= [code] if isinstance(code,str) else code

    # code checking
    code = QA_util_code_tolist(code)

    if QA_util_date_valid(end):

        __data = []
        cursor = collections.find({
            'code': {'$in': code}, "date_stamp": {
                "$lte": QA_util_date_stamp(end),
                "$gte": QA_util_date_stamp(start)}}, batch_size=10000)
        #res=[QA_util_dict_remove_key(data, '_id') for data in cursor]

        res = pd.DataFrame([item for item in cursor])
        try:
            res = res.drop('_id', axis=1).assign(volume=res.vol, date=pd.to_datetime(
                res.date)).drop_duplicates((['date', 'code'])).query('volume>1').set_index('date', drop=False)
            res = res.ix[:, ['code', 'open', 'high', 'low',
                             'close', 'volume', 'amount', 'date']]
        except:
            res = None
        if format in ['P', 'p', 'pandas', 'pd']:
            return res
        elif format in ['json', 'dict']:
            return QA_util_to_json_from_pandas(res)
        # 多种数据格式
        elif format in ['n', 'N', 'numpy']:
            return numpy.asarray(res)
        elif format in ['list', 'l', 'L']:
            return numpy.asarray(res).tolist()
        else:
            print("QA Error QA_fetch_stock_day format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
            return None
    else:
        QA_util_log_info(
            'QA Error QA_fetch_stock_day data parameter start=%s end=%s is not right' % (start, end))

def QA_fetch_stock_day_ts(code, start, end, format='pd', frequence='day', collections=DATABASE.stock_day_ts):
    '获取股票日线'
    start = str(start)[0:10]
    end = str(end)[0:10]
    #code= [code] if isinstance(code,str) else code

    # code checking
    code = QA_util_code_tolist(code)

    if QA_util_date_valid(end):

        __data = []
        cursor = collections.find({
            'code': {'$in': code}, "date_stamp": {
                "$lte": QA_util_date_stamp(end),
                "$gte": QA_util_date_stamp(start)}})
        #res=[QA_util_dict_remove_key(data, '_id') for data in cursor]

        res = pd.DataFrame([item for item in cursor])
        try:
            # drop '_id', change 'date' from str to datetime, set 'date' to index
            res = res.drop('_id', axis=1).assign(volume=res.volume).query('volume>1').assign(date=pd.to_datetime(
                res.date)).drop_duplicates((['date', 'code'])).set_index('date', drop=False)
            res = res.ix[:, ['code', 'open', 'high', 'low',
                             'close', 'volume', 'date']]
        except:
            res = None
        if format in ['P', 'p', 'pandas', 'pd']:
            return res
        elif format in ['json', 'dict']:
            return QA_util_to_json_from_pandas(res)
        # 多种数据格式
        elif format in ['n', 'N', 'numpy']:
            return numpy.asarray(res)
        elif format in ['list', 'l', 'L']:
            return numpy.asarray(res).tolist()
        else:
            print("QA Error QA_fetch_stock_day_ts format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
            return None
    else:
        QA_util_log_info(
            'QA Error QA_fetch_stock_day_ts data parameter start=%s end=%s is not right' % (start, end))

def QA_fetch_index_day_ts(code, start, end, format='pd', frequence='day', collections=DATABASE.index_day_ts):
    '获取股票日线'
    start = str(start)[0:10]
    end = str(end)[0:10]
    #code= [code] if isinstance(code,str) else code

    # code checking
    code = QA_util_code_tolist(code)

    if QA_util_date_valid(end):

        __data = []
        cursor = collections.find({
            'code': {'$in': code}, "date_stamp": {
                "$lte": QA_util_date_stamp(end),
                "$gte": QA_util_date_stamp(start)}})
        #res=[QA_util_dict_remove_key(data, '_id') for data in cursor]

        res = pd.DataFrame([item for item in cursor])
        try:
            # drop '_id', change 'date' from str to datetime, set 'date' to index
            res = res.drop('_id', axis=1).assign(volume=res.volume).query('volume>1').assign(date=pd.to_datetime(
                res.date)).drop_duplicates((['date', 'code'])).set_index('date', drop=False)
            res = res.ix[:, ['code', 'open', 'high', 'low',
                             'close', 'volume', 'date']]
        except:
            res = None
        if format in ['P', 'p', 'pandas', 'pd']:
            return res
        elif format in ['json', 'dict']:
            return QA_util_to_json_from_pandas(res)
        # 多种数据格式
        elif format in ['n', 'N', 'numpy']:
            return numpy.asarray(res)
        elif format in ['list', 'l', 'L']:
            return numpy.asarray(res).tolist()
        else:
            print("QA Error QA_fetch_index_day_ts format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
            return None
    else:
        QA_util_log_info(
            'QA Error QA_fetch_index_day_ts data parameter start=%s end=%s is not right' % (start, end))

def QA_fetch_fin_indicator(code, start, end, keys='all', format='pd', collections=DATABASE.df_financial_indicator):
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    code = QA_util_code_tolist(code)

    if QA_util_date_valid(end):
        cursor = collections.find({
            'code': {'$in': code}, "date_stamp": {
                "$lte": QA_util_date_stamp(end),
                "$gte": QA_util_date_stamp(start)}})

        res = pd.DataFrame([item for item in cursor])
        try:
            # drop '_id', change 'date' from str to datetime, set 'date' to index
            res = res.drop('_id', axis=1).assign(date=pd.to_datetime(
                    res.date)).drop_duplicates((['date', 'code'])).set_index('date', drop=False)
            if keys != 'all':
                res = res.ix[:, keys]
        except:
            res = None
        
        if format in ['P', 'p', 'pandas', 'pd']:
            return res
        elif format in ['json', 'dict']:
            return QA_util_to_json_from_pandas(res)
        # 多种数据格式
        elif format in ['n', 'N', 'numpy']:
            return numpy.asarray(res)
        elif format in ['list', 'l', 'L']:
            return numpy.asarray(res).tolist()
        else:
            print("QA Error QA_fetch_fin_indicator format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
            return None
    else:
        QA_util_log_info(
            'QA Error QA_fetch_fin_indicator data parameter start=%s end=%s is not right' % (start, end))

def QA_fetch_tech_indicator(code, start, end, keys='all', format='pd', collections=DATABASE.df_tech_indicator):
    """
    return: from start to end, doesn't exclude vol==0 dates in between, those date are continuous
            and aligned with sh index already
    """
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    code = QA_util_code_tolist(code)

    if QA_util_date_valid(end):
        cursor = collections.find({
            'code': {'$in': code}, "date_stamp": {
                "$lte": QA_util_date_stamp(end),
                "$gte": QA_util_date_stamp(start)}})

        res = pd.DataFrame([item for item in cursor])
        try:
            # drop '_id', change 'date' from str to datetime, set 'date' to index
            res = res.drop('_id', axis=1).assign(date=pd.to_datetime(
                    res.date)).drop_duplicates((['date', 'code'])).set_index('date', drop=False)
            if keys != 'all':
                if isinstance(keys, str):
                    res = res.ix[:, [keys]]
                elif isinstance(keys, list):
                    res = res.ix[:, keys]
        except:
            res = None
        
        if format in ['P', 'p', 'pandas', 'pd']:
            return res
        elif format in ['json', 'dict']:
            return QA_util_to_json_from_pandas(res)
        # 多种数据格式
        elif format in ['n', 'N', 'numpy']:
            return numpy.asarray(res)
        elif format in ['list', 'l', 'L']:
            return numpy.asarray(res).tolist()
        else:
            print("QA Error QA_fetch_tech_indicator format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
            return None
    else:
        QA_util_log_info(
            'QA Error QA_fetch_tech_indicator data parameter start=%s end=%s is not right' % (start, end))

def QA_fetch_stock_pure_tech_indicator(code, start, end, vol='non-zero', keys='all', format='pd', collections=DATABASE.stock_tech_indicator_3):
    """
    return: from start to end, doesn't exclude vol==0 dates in between, those date are continuous
            and aligned with sh index already
    """
    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    code = QA_util_code_tolist(code)

    if QA_util_date_valid(end):
        if vol == 'non-zero':
            cursor = collections.find({
                'code': {'$in': code}, "date_stamp": {
                    "$lte": QA_util_date_stamp(end),
                    "$gte": QA_util_date_stamp(start)},
                    'volume': {"$gt": 0}})
        else:
            cursor = collections.find({
                'code': {'$in': code}, "date_stamp": {
                    "$lte": QA_util_date_stamp(end),
                    "$gte": QA_util_date_stamp(start)}})

        res = pd.DataFrame([item for item in cursor])
        try:
            # drop '_id', change 'date' from str to datetime, set 'date' to index
            res = res.drop('_id', axis=1).assign(date=pd.to_datetime(
                    res.date)).drop_duplicates((['date', 'code'])).set_index('date', drop=False)
            if keys != 'all':
                if isinstance(keys, str):
                    res = res.ix[:, [keys]]
                elif isinstance(keys, list):
                    res = res.ix[:, keys]
        except:
            res = None
        
        if format in ['P', 'p', 'pandas', 'pd']:
            return res
        elif format in ['json', 'dict']:
            return QA_util_to_json_from_pandas(res)
        # 多种数据格式
        elif format in ['n', 'N', 'numpy']:
            return numpy.asarray(res)
        elif format in ['list', 'l', 'L']:
            return numpy.asarray(res).tolist()
        else:
            print("QA Error QA_fetch_tech_indicator format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
            return None
    else:
        QA_util_log_info(
            'QA Error QA_fetch_tech_indicator data parameter start=%s end=%s is not right' % (start, end))

def convert_fin_code(stock_code):
    if stock_code[:2] in ['60']:
        postfix = '.SH'
    elif stock_code[:3] in ['000']:
        postfix = '.SZ'
    elif stock_code[:3] in ['002']:
        postfix = '.SZ'
    elif stock_code[:3] in ['300']:
        postfix = '.SZ'
    else:
        print("error with converting fin code for {}".format(stock_code))
    return str(stock_code) + postfix

def QA_fetch_stock_fin_indicator_ts(code, start, end, keys='all', format='pd', collections=DATABASE.stock_financial_ts):

    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    #code = QA_util_code_tolist(code)

    if QA_util_date_valid(end):

        code = convert_fin_code(code)

        cursor = collections.find({
                'ts_code': str(code),
                "date_stamp": {
                "$lte": QA_util_date_stamp(end),
                "$gte": QA_util_date_stamp(start)}})

        res = pd.DataFrame([item for item in cursor])
        try:
            # drop '_id', change 'date' from str to datetime, set 'date' to index
            res = res.drop('_id', axis=1).assign(date=pd.to_datetime(
                    res.date)).set_index('date', drop=False)

        except:
            res = None
        
        if format in ['P', 'p', 'pandas', 'pd']:
            return res
        elif format in ['json', 'dict']:
            return QA_util_to_json_from_pandas(res)
        # 多种数据格式
        elif format in ['n', 'N', 'numpy']:
            return numpy.asarray(res)
        elif format in ['list', 'l', 'L']:
            return numpy.asarray(res).tolist()
        else:
            print("QA Error QA_fetch_stock_fin_indicator_ts format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
            return None
    else:
        QA_util_log_info(
            'QA Error QA_fetch_stock_fin_indicator_ts data parameter start=%s end=%s is not right' % (start, end))


def QA_fetch_tech_indicator_normalized(code, start, end, keys='all', format='pd', collections=DATABASE.df_tech_indicator_normalized):
    """
    keys: 'all' or ['indicator1', indicator2', ....]
    return: from start to end, doesn't exclude vol==0 dates in between, those date are continuous
            and aligned with sh index already
    """
    start = str(start)[0:10]
    end = str(end)[0:10]

    if keys == 'all':
        keys = [ 'OCF_G_q', 'OCF_cal_G_q', 'OCF_netprofit_r_q', 'PB', 'PE', 'PEG', 'PEG_cut',
       'PE_cut', 'PNCF', 'POCF', 'PS', 'ROA_q', 'ROE_G_q', 'ROE_q', 'RSI_20',
       'STD_12m', 'STD_1m', 'STD_3m', 'STD_6m', 'assetturnover_q',
       'bias_turn_1m', 'bias_turn_3m', 'bias_turn_6m', 'captital_tot', 
       'debitequityratio_q', 'exp_wgt_return_12m', 'exp_wgt_return_1m', 'exp_wgt_return_3m',
       'exp_wgt_return_6m', 'fin_leverage_q', 'grossprofit_q', 'profit_G_q', 'profit_cut_G_q',
       'profitmargin_q', 'return_12m', 'return_1m', 'return_3m', 'return_6m',
       'rs', 'sales_G_q', 'turn_12m', 'turn_1m', 'turn_24m', 'turn_3m',
       'turn_6m', 'turnover_d',  'wgt_return_12m', 'wgt_return_1m',
       'wgt_return_3m', 'wgt_return_6m']

    # code checking
    #code = QA_util_code_tolist(code)

    if QA_util_date_valid(end):
        cursor = collections.find({
            'key':  {'$in': keys}, "date_stamp": {
                "$lte": QA_util_date_stamp(end),
                "$gte": QA_util_date_stamp(start)}},
                { str(code): 1, 'date': 1,  'key': 1})

        res = pd.DataFrame([item for item in cursor])
        try:
            # drop '_id', change 'date' from str to datetime, set 'date' to index
            res = res.drop('_id', axis=1).assign(date=pd.to_datetime(
                    res.date)).set_index('date', drop=False)
            cols = ['key', str(code)]
            res = res.ix[:, cols]
            # pivot on 'key' to make each column a indicator
            res = res.pivot(columns='key', values=code)
        except:
            res = None
        
        if format in ['P', 'p', 'pandas', 'pd']:
            return res
        elif format in ['json', 'dict']:
            return QA_util_to_json_from_pandas(res)
        # 多种数据格式
        elif format in ['n', 'N', 'numpy']:
            return numpy.asarray(res)
        elif format in ['list', 'l', 'L']:
            return numpy.asarray(res).tolist()
        else:
            print("QA Error QA_fetch_tech_indicator format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
            return None
    else:
        QA_util_log_info(
            'QA Error QA_fetch_tech_indicator data parameter start=%s end=%s is not right' % (start, end))

def QA_fetch_stock_min(code, start, end, format='numpy', frequence='1min', collections=DATABASE.stock_min):
    '获取股票分钟线'
    if frequence in ['1min', '1m']:
        frequence = '1min'
    elif frequence in ['5min', '5m']:
        frequence = '5min'
    elif frequence in ['15min', '15m']:
        frequence = '15min'
    elif frequence in ['30min', '30m']:
        frequence = '30min'
    elif frequence in ['60min', '60m']:
        frequence = '60min'
    else:
        print("QA Error QA_fetch_stock_min parameter frequence=%s is none of 1min 1m 5min 5m 15min 15m 30min 30m 60min 60m" % frequence)

    __data = []
    # code checking
    code = QA_util_code_tolist(code)

    cursor = collections.find({
        'code': {'$in': code}, "time_stamp": {
            "$gte": QA_util_time_stamp(start),
            "$lte": QA_util_time_stamp(end)
        }, 'type': frequence
    }, batch_size=10000)

    res = pd.DataFrame([item for item in cursor])
    try:
        res = res.drop('_id', axis=1).assign(volume=res.vol, datetime=pd.to_datetime(
            res.datetime)).query('volume>1').drop_duplicates(['datetime', 'code']).set_index('datetime', drop=False)
        # return res
    except:
        res = None
    if format in ['P', 'p', 'pandas', 'pd']:
        return res
    elif format in ['json', 'dict']:
        return QA_util_to_json_from_pandas(res)
    # 多种数据格式
    elif format in ['n', 'N', 'numpy']:
        return numpy.asarray(res)
    elif format in ['list', 'l', 'L']:
        return numpy.asarray(res).tolist()
    else:
        print("QA Error QA_fetch_stock_min format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
        return None


def QA_fetch_trade_date():
    '获取交易日期'
    return trade_date_sse


def QA_fetch_stock_list(collections=DATABASE.stock_list):
    '获取股票列表'
    return [item for item in collections.find()]

def QA_fetch_index_list(collections=DATABASE.index_list):
    '获取指数列表'
    return [item for item in collections.find()]

def QA_fetch_stock_terminated(collections=DATABASE.stock_terminated):
    '获取股票基本信息 , 已经退市的股票列表'
    items = [item for item in collections.find()]
    # 🛠todo  转变成 dataframe 类型数据
    return items


def QA_fetch_stock_basic_info_tushare(collections=DATABASE.stock_info_tushare):
    '''
    purpose:
        tushare 股票列表数据库

        code,代码
        name,名称
        industry,所属行业
        area,地区
        pe,市盈率
        outstanding,流通股本(亿)
        totals,总股本(亿)
        totalAssets,总资产(万)
        liquidAssets,流动资产
        fixedAssets,固定资产
        reserved,公积金
        reservedPerShare,每股公积金
        esp,每股收益
        bvps,每股净资
        pb,市净率
        timeToMarket,上市日期
        undp,未分利润
        perundp, 每股未分配
        rev,收入同比(%)
        profit,利润同比(%)
        gpr,毛利率(%)
        npr,净利润率(%)
        holders,股东人数

        add by tauruswang,

    :param collections: stock_info_tushare 集合
    :return:
    '''
    '获取股票基本信息'
    items = [item for item in collections.find()]
    # 🛠todo  转变成 dataframe 类型数据
    return items


def QA_fetch_stock_to_market_date(stock_code):
    '''
    根据tushare 的数据库查找上市的日期
    :param stock_code: '600001'
    :return: string 上市日期 eg： '2018-05-15'
    '''
    items = QA_fetch_stock_basic_info_tushare()
    for row in items:
        if row['code'] == stock_code:
            return row['timeToMarket']


def QA_fetch_stock_full(date, format='numpy', collections=DATABASE.stock_day):
    '获取全市场的某一日的数据'
    Date = str(date)[0:10]
    if QA_util_date_valid(Date) is True:

        __data = []
        for item in collections.find({
                "date_stamp": QA_util_date_stamp(Date)}, batch_size=10000):
            __data.append([str(item['code']), float(item['open']), float(item['high']), float(
                item['low']), float(item['close']), float(item['vol']), item['date']])
        # 多种数据格式
        if format in ['n', 'N', 'numpy']:
            __data = numpy.asarray(__data)
        elif format in ['list', 'l', 'L']:
            __data = __data
        elif format in ['P', 'p', 'pandas', 'pd']:
            __data = DataFrame(__data, columns=[
                'code', 'open', 'high', 'low', 'close', 'volume', 'date'])
            __data['date'] = pd.to_datetime(__data['date'])
            __data = __data.set_index('date', drop=False)
        else:
            print("QA Error QA_fetch_stock_full format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)

        return __data
    else:
        QA_util_log_info(
            'QA Error QA_fetch_stock_full data parameter date=%s not right' % date)


def QA_fetch_index_day(code, start, end, format='numpy', collections=DATABASE.index_day):
    '获取指数日线'
    start = str(start)[0:10]
    end = str(end)[0:10]
    code = QA_util_code_tolist(code)
    if QA_util_date_valid(end) == True:

        __data = []
        cursor = collections.find({
            'code': {'$in': code}, "date_stamp": {
                "$lte": QA_util_date_stamp(end),
                "$gte": QA_util_date_stamp(start)}}, batch_size=10000)
        if format in ['dict', 'json']:
            return [data for data in cursor]
        for item in cursor:

            __data.append([str(item['code']), float(item['open']), float(item['high']), float(
                item['low']), float(item['close']), int(item['up_count']), int(item['down_count']), float(item['vol']), float(item['amount']), item['date']])

        # 多种数据格式
        if format in ['n', 'N', 'numpy']:
            __data = numpy.asarray(__data)
        elif format in ['list', 'l', 'L']:
            __data = __data
        elif format in ['P', 'p', 'pandas', 'pd']:
            __data = DataFrame(
                __data, columns=['code', 'open', 'high', 'low', 'close', 'up_count', 'down_count', 'volume', 'amount', 'date'])
            __data['date'] = pd.to_datetime(__data['date'])
            __data = __data.set_index('date', drop=False)
        else:
            print("QA Error QA_fetch_index_day format parameter %s is none of  \"P, p, pandas, pd , n, N, numpy !\" " % format)
        return __data
    else:
        QA_util_log_info('QA something wrong with date')

def QA_fetch_block_index_day(code, start, end, format='numpy', collections=DATABASE.block_index_day):
    '获取指数日线'
    start = str(start)[0:10]
    end = str(end)[0:10]
    code = QA_util_code_tolist(code)
    if QA_util_date_valid(end) == True:

        __data = []
        cursor = collections.find({
            'code': {'$in': code}, "date_stamp": {
                "$lte": QA_util_date_stamp(end),
                "$gte": QA_util_date_stamp(start)}})
        if format in ['dict', 'json']:
            return [data for data in cursor]
        for item in cursor:

            __data.append([str(item['code']), float(item['open']), float(item['high']), float(
                item['low']), float(item['close']), float(item['vol']), item['date']])

        # 多种数据格式
        if format in ['n', 'N', 'numpy']:
            __data = numpy.asarray(__data)
        elif format in ['list', 'l', 'L']:
            __data = __data
        elif format in ['P', 'p', 'pandas', 'pd']:
            __data = DataFrame(
                __data, columns=['code', 'open', 'high', 'low', 'close', 'volume', 'date'])
            __data['date'] = pd.to_datetime(__data['date'])
            __data = __data.set_index('date', drop=False)
        else:
            print("QA Error QA_fetch_block_index_day format parameter %s is none of  \"P, p, pandas, pd , n, N, numpy !\" " % format)
        return __data
    else:
        QA_util_log_info('QA something wrong with date')


def QA_fetch_index_min(
        code,
        start, end,
        format='numpy',
        frequence='1min',
        collections=DATABASE.index_min):
    '获取股票分钟线'
    if frequence in ['1min', '1m']:
        frequence = '1min'
    elif frequence in ['5min', '5m']:
        frequence = '5min'
    elif frequence in ['15min', '15m']:
        frequence = '15min'
    elif frequence in ['30min', '30m']:
        frequence = '30min'
    elif frequence in ['60min', '60m']:
        frequence = '60min'
    __data = []
    code = QA_util_code_tolist(code)
    cursor = collections.find({
        'code': {'$in': code}, "time_stamp": {
            "$gte": QA_util_time_stamp(start),
            "$lte": QA_util_time_stamp(end)
        }, 'type': frequence
    }, batch_size=10000)
    if format in ['dict', 'json']:
        return [data for data in cursor]
    for item in cursor:

        __data.append([str(item['code']), float(item['open']), float(item['high']), float(
            item['low']), float(item['close']), int(item['up_count']), int(item['down_count']), float(item['vol']), float(item['amount']), item['datetime'], item['time_stamp'], item['date']])

    __data = DataFrame(__data, columns=[
        'code', 'open', 'high', 'low', 'close', 'up_count', 'down_count', 'volume', 'amount', 'datetime', 'time_stamp', 'date'])

    __data['datetime'] = pd.to_datetime(__data['datetime'])
    __data = __data.set_index('datetime', drop=False)
    if format in ['numpy', 'np', 'n']:
        return numpy.asarray(__data)
    elif format in ['list', 'l', 'L']:
        return numpy.asarray(__data).tolist()
    elif format in ['P', 'p', 'pandas', 'pd']:
        return __data


def QA_fetch_future_day():
    raise NotImplementedError


def QA_fetch_future_min():
    raise NotImplementedError


def QA_fetch_future_list(collections=DATABASE.future_list):
    '获取期货列表'
    return [item for item in collections.find()]

def QA_fetch_future_tick():
    raise NotImplementedError


def QA_fetch_stock_xdxr(code, format='pd', collections=DATABASE.stock_xdxr):
    '获取股票除权信息/数据库'
    code = QA_util_code_tolist(code)
    data = pd.DataFrame([item for item in collections.find(
        {'code':  {'$in': code}}, batch_size=10000)]).drop(['_id'], axis=1)
    data['date'] = pd.to_datetime(data['date'])
    return data.set_index('date', drop=False)


def QA_fetch_backtest_info(user=None, account_cookie=None, strategy=None, stock_list=None, collections=DATABASE.backtest_info):

    return QA_util_to_json_from_pandas(pd.DataFrame([item for item in collections.find(QA_util_to_json_from_pandas(pd.DataFrame([user, account_cookie, strategy, stock_list], index=['user', 'account_cookie', 'strategy', 'stock_list']).dropna().T)[0])]).drop(['_id'], axis=1))


def QA_fetch_backtest_history(cookie=None, collections=DATABASE.backtest_history):
    return QA_util_to_json_from_pandas(pd.DataFrame([item for item in collections.find(QA_util_to_json_from_pandas(pd.DataFrame([cookie], index=['cookie']).dropna().T)[0])]).drop(['_id'], axis=1))


def QA_fetch_stock_block(code=None, format='pd', collections=DATABASE.stock_block):
    if code is not None:
        code = QA_util_code_tolist(code)
        data = pd.DataFrame([item for item in collections.find(
            {'code': {'$in': code}}, batch_size=10000)]).drop(['_id'], axis=1)
        return data.set_index('code', drop=False)
    else:
        data = pd.DataFrame(
            [item for item in collections.find()]).drop(['_id'], axis=1)
        return data.set_index('code', drop=False)


def QA_fetch_stock_info(code, format='pd', collections=DATABASE.stock_info):
    code = QA_util_code_tolist(code)
    try:
        data = pd.DataFrame([item for item in collections.find(
            {'code':  {'$in': code}}, batch_size=10000)]).drop(['_id'], axis=1)
        #data['date'] = pd.to_datetime(data['date'])
        return data.set_index('code', drop=False)
    except Exception as e:
        QA_util_log_info(e)
        return None


def QA_fetch_stock_name(code, collections=DATABASE.stock_list):
    try:
        return collections.find_one({'code': code})['name']
    except Exception as e:
        QA_util_log_info(e)


def QA_fetch_quotation(code, date=datetime.date.today(), db=DATABASE):
    '获取某一只实时5档行情的存储结果'
    try:
        collections = db.get_collection(
            'realtime_{}'.format(date))
        data = pd.DataFrame([item for item in collections.find(
            {'code': code}, batch_size=10000)]).drop(['_id'], axis=1)
        return data.assign(date=data.datetime.apply(lambda x: str(x)[0:10]), datetime=pd.to_datetime(data.datetime)) \
            .set_index('datetime', drop=False).sort_index()
    except Exception as e:
        raise e


def QA_fetch_quotations(date=datetime.date.today(), db=DATABASE):
    '获取全部实时5档行情的存储结果'
    try:
        collections = db.get_collection(
            'realtime_{}'.format(date))
        data = pd.DataFrame([item for item in collections.find(
            {}, batch_size=10000)]).drop(['_id'], axis=1)
        return data.assign(date=data.datetime.apply(lambda x: str(x)[0:10])).assign(datetime=pd.to_datetime(data.datetime)).set_index('datetime', drop=False).sort_index()
    except Exception as e:
        raise e


def QA_fetch_account(message={}, db=DATABASE):
    """get the account

    Arguments:
        query_mes {[type]} -- [description]

    Keyword Arguments:
        collection {[type]} -- [description] (default: {DATABASE})

    Returns:
        [type] -- [description]
    """
    collection = DATABASE.account
    return [QA_util_dict_remove_key(res, '_id') for res in collection.find(message)]


def QA_fetch_user(user_cookie, db=DATABASE):
    """
    get the user

    Arguments:
        user_cookie : str the unique cookie_id for a user
    Keyword Arguments:
        db: database for query

    Returns:
        list ---  [ACCOUNT]
    """
    collection = DATABASE.account

    return [QA_util_dict_remove_key(res, '_id') for res in collection.find({'user_cookie': user_cookie})]


def QA_fetch_lhb(date, db=DATABASE):
    '获取某一天龙虎榜数据'
    try:
        collections = db.lhb
        return pd.DataFrame([item for item in collections.find(
            {'date': date})]).drop(['_id'], axis=1).set_index('code', drop=False).sort_index()
    except Exception as e:
        raise e


def QA_fetch_financial_report(code, report_date, ltype='CN', db=DATABASE):
    """获取专业财务报表

    Arguments:
        code {[type]} -- [description]
        report_date {[type]} -- [description]

    Keyword Arguments:
        ltype {str} -- [description] (default: {'EN'})
        db {[type]} -- [description] (default: {DATABASE})

    Raises:
        e -- [description]

    Returns:
        pd.DataFrame -- [description]
    """

    if isinstance(code, str):
        code = [code]
    if isinstance(report_date, str):
        report_date = [QA_util_date_str2int(report_date)]
    elif isinstance(report_date, int):
        report_date = [report_date]
    elif isinstance(report_date, list):
        report_date = [QA_util_date_str2int(item) for item in report_date]

    collection = db.financial
    CH_columns = [item for item in sorted(list(financial_dict.keys()))]
    CH_columns.extend(['277', '278', '279', '280', '281', '282', '_id', 'code',
                       'report_date'])
    CH_columns = pd.Index(CH_columns)
    #EN_columns = list(financial_dict.values())
    EN_columns = [financial_dict[key] for key in sorted(list(financial_dict.keys()))]
    EN_columns.extend(['277', '278', '279', '280', '281', '282', '_id', 'code',
                       'report_date'])
    EN_columns = pd.Index(EN_columns)

    try:
        if code is not None and report_date is not None:
            data = [item for item in collection.find(
                {'code': {'$in': code}, 'report_date': {'$in': report_date}}, batch_size=10000)]
        elif code is None and report_date is not None:
            data = [item for item in collection.find(
                {'report_date': {'$in': report_date}}, batch_size=10000)]
        elif code is not None and report_date is None:
            data = [item for item in collection.find(
                {'code': {'$in': code}}, batch_size=10000)]
        else:
            data = [item for item in collection.find()]
        if len(data) > 0:
            res_pd = pd.DataFrame(data)

            if ltype in ['CH', 'CN']:
                res_pd.columns = CH_columns
            elif ltype is 'EN':
                res_pd.columns = EN_columns

            if res_pd.report_date.dtype == numpy.int64:
                res_pd.report_date = pd.to_datetime(
                    res_pd.report_date.apply(QA_util_date_int2str))
            else:
                res_pd.report_date=pd.to_datetime(res_pd.report_date)
            
            #return res_pd.replace(-4.039810335e+34, numpy.nan).set_index(['report_date', 'code'], drop=False)
            return res_pd.replace(-4.039810335e+34, numpy.nan).set_index(['report_date'], drop=False)
        else:
            return None
    except Exception as e:
        raise e


def QA_fetch_balance_test(code, start='all', end=None, format='pd', collections=DATABASE.fin_balance_cn):
    if start == 'all':
        start = '1990-01-01'
    if end is None:
        end = str(datetime.date.today())

    start = str(start)[0:10]
    end = str(end)[0:10]

    # code checking
    code = QA_util_code_tolist(code)

    if QA_util_date_valid(end):
        cursor = collections.find({
            'code': {'$in': code}})

        res = pd.DataFrame([item for item in cursor])
        try:
            # drop '_id', change 'date' from str to datetime, set 'date' to index
            res = res.drop('_id', axis=1).assign(date=pd.to_datetime(
                    res['报表日期'])).drop_duplicates((['date', 'code'])).set_index('date', drop=False)
            
        except:
            res = None
        
        if format in ['P', 'p', 'pandas', 'pd']:
            return res
        elif format in ['json', 'dict']:
            return QA_util_to_json_from_pandas(res)
        # 多种数据格式
        elif format in ['n', 'N', 'numpy']:
            return numpy.asarray(res)
        elif format in ['list', 'l', 'L']:
            return numpy.asarray(res).tolist()
        else:
            print("QA Error QA_fetch_fin_indicator format parameter %s is none of  \"P, p, pandas, pd , json, dict , n, N, numpy, list, l, L, !\" " % format)
            return None
    else:
        QA_util_log_info(
            'QA Error QA_fetch_fin_indicator data parameter start=%s end=%s is not right' % (start, end))

if __name__ == '__main__':
    print(QA_fetch_lhb('2006-07-03'))
