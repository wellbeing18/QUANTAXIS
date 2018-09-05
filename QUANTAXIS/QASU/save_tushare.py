# coding:utf-8
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
import json
import re
import time

import tushare as ts
from multiprocessing import Pool
import concurrent
from concurrent.futures import ThreadPoolExecutor

import pymongo

from QUANTAXIS.QAFetch.QATushare import (QA_fetch_get_stock_day,
                                         QA_fetch_get_index_day,
                                         QA_fetch_get_stock_info,
                                         QA_fetch_get_stock_list,
                                         QA_fetch_get_trade_date,
                                         QA_fetch_get_lhb)
from QUANTAXIS.QAUtil import (QA_util_date_stamp, QA_util_log_info,
                              QA_util_time_stamp, QA_util_to_json_from_pandas,
                              trade_date_sse)
from QUANTAXIS.QAUtil.QASetting import DATABASE
from QUANTAXIS.QAUtil import (DATABASE, QA_util_get_next_day,
                              QA_util_get_real_date, QA_util_log_info,
                              QA_util_to_json_from_pandas, trade_date_sse)

import tushare as QATs

def now_time():
    return str(QA_util_get_real_date(str(datetime.date.today() - datetime.timedelta(days=1)), trade_date_sse, -1)) + \
        ' 17:00:00' if datetime.datetime.now().hour < 15 else str(QA_util_get_real_date(
            str(datetime.date.today()), trade_date_sse, -1)) + ' 15:00:00'

def QA_SU_save_stock_day(client=DATABASE):
    df = ts.get_stock_basics()
    coll_stock_day = client.stock_day_ts
    coll_stock_day.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)])

    err = []

    def saving_work(code, coll_stock_day):
        try:
            QA_util_log_info('Now Saving ==== %s' % (code))

            ref = coll_stock_day.find({'code': str(code)[:6]})
            end_date = str(now_time())[:10]

            if ref.count() > 0:
                start_date = ref[ref.count() - 1]['date']
                QA_util_log_info('UPDATE_STOCK_DAY \n Trying updating {} from {} to {}'.format(
                                    code, start_date, end_date))
                if start_date != end_date:
                    start_date = QA_util_get_next_day(start_date)
                    QA_util_log_info('UPDATE_STOCK_DAY \n Trying updating {} from {} to {}'.format(
                                    code, start_date, end_date))
                    data_json = QA_fetch_get_stock_day(str(code), start=start_date, end=end_date, type_='json')
                    coll_stock_day.insert_many(data_json)
            else:
                start_date = '1990-01-01'
                data_json = QA_fetch_get_stock_day(str(code), start=start_date, type_='json')
                coll_stock_day.insert_many(data_json)
        except Exception as e:
            QA_util_log_info('error in saving ==== %s' % str(code))
            err.append(code)
            print("The exception is {}".format(str(e)))

    # bwang: multi-thread
    executor = ThreadPoolExecutor(max_workers=30)
    res = {executor.submit(saving_work,  df.index[i_], coll_stock_day) for i_ in range(len(df.index))}

    count = 0
    for i_ in concurrent.futures.as_completed(res):
        QA_util_log_info('The {} of Total {}'.format(count, len(df.index)))

        strProgress = 'DOWNLOAD PROGRESS {} '.format(str(float(count / len(df.index) * 100))[0:4] + '%')
        intProgress = int(count / len(df.index) * 10000.0)
        QA_util_log_info(strProgress, ui_progress_int_value=intProgress)
        count = count + 1
    if len(err) < 1:
        QA_util_log_info('SUCCESS')
    else:
        QA_util_log_info(' ERROR CODE \n ')
        QA_util_log_info(err)

    """
    #pool = Pool(3)

    for i_ in range(len(df.index)):
        QA_util_log_info('The %s of Total %s' % (i_, len(df.index)))
        QA_util_log_info('DOWNLOAD PROGRESS %s ' % str(
            float(i_ / len(df.index) * 100))[0:4] + '%')
        saving_work(df.index[i_], coll_stock_day)
        #pool.apply_async(saving_work, args=(df.index[i_], coll_stock_day))

    #pool.close()
    #pool.join()
    """
    #saving_work('hs300')
    #saving_work('sz50')

def QA_SU_save_index_day(client=DATABASE):
    index_list = ['000001', '399001', '399005', '399006']
    coll_index_day = client.index_day_ts
    coll_index_day.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)])

    err = []

    def saving_work(code, coll_index_day):
        try:
            QA_util_log_info('Now Saving ==== %s' % (code))

            ref = coll_index_day.find({'code': str(code)[:6]})
            end_date = str(now_time())[:10]

            if ref.count() > 0:
                start_date = ref[ref.count() - 1]['date']
                QA_util_log_info('UPDATE_INDEX_DAY \n Trying updating {} from {} to {}'.format(
                                    code, start_date, end_date))
                if start_date != end_date:
                    start_date = QA_util_get_next_day(start_date)
                    data_json = QA_fetch_get_index_day(str(code), start=start_date, end=end_date, type_='json')
                    coll_index_day.insert_many(data_json)
            else:
                start_date = '1990-01-01'
                QA_util_log_info('UPDATE_INDEX_DAY \n Trying updating {} from {} to {}'.format(
                                    code, start_date, end_date))
                data_json = QA_fetch_get_index_day(str(code), start=start_date, type_='json')
                coll_index_day.insert_many(data_json)
        except Exception as e:
            QA_util_log_info('error in saving index ==== %s' % str(code))
            err.append(code)
            print("The exception is {}".format(str(e)))

    # bwang: multi-thread
    executor = ThreadPoolExecutor(max_workers=5)
    res = {executor.submit(saving_work,  index_list[i], coll_index_day) for i in range(len(index_list))}

    count = 0
    for i_ in concurrent.futures.as_completed(res):
        QA_util_log_info('The {} of Total {}'.format(count, len(index_list)))

        strProgress = 'DOWNLOAD PROGRESS {} '.format(str(float(count / len(index_list) * 100))[0:4] + '%')
        intProgress = int(count / len(index_list) * 10000.0)
        QA_util_log_info(strProgress, ui_progress_int_value=intProgress)
        count = count + 1
    if len(err) < 1:
        QA_util_log_info('SUCCESS')
    else:
        QA_util_log_info(' ERROR CODE \n ')
        QA_util_log_info(err)


def QA_SU_save_stock_list(client=DATABASE):
    data = QA_fetch_get_stock_list()
    date = str(datetime.date.today())
    date_stamp = QA_util_date_stamp(date)
    coll = client.stock_info_tushare
    coll.insert({'date': date, 'date_stamp': date_stamp,
                 'stock': {'code': data}})


def QA_SU_save_stock_terminated(client=DATABASE):
    '''
    获取已经被终止上市的股票列表，数据从上交所获取，目前只有在上海证券交易所交易被终止的股票。
    collection：
        code：股票代码 name：股票名称 oDate:上市日期 tDate:终止上市日期
    :param client:
    :return: None
    '''

    # 🛠todo 已经失效从wind 资讯里获取
    # 这个函数已经失效
    print("！！！ tushare 这个函数已经失效！！！")
    df = QATs.get_terminated()
    #df = QATs.get_suspended()
    print(" Get stock terminated from tushare,stock count is %d  (终止上市股票列表)" % len(df))
    coll = client.stock_terminated
    client.drop_collection(coll)
    json_data = json.loads(df.reset_index().to_json(orient='records'))
    coll.insert(json_data)
    print(" 保存终止上市股票列表 到 stock_terminated collection， OK")


def QA_SU_save_stock_info_tushare(client=DATABASE):
    '''
        获取 股票的 基本信息，包含股票的如下信息

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

        add by tauruswang

    在命令行工具 quantaxis 中输入 save stock_info_tushare 中的命令
    :param client:
    :return:
    '''
    df = QATs.get_stock_basics()
    print(" Get stock info from tushare,stock count is %d" % len(df))
    coll = client.stock_info_tushare
    client.drop_collection(coll)
    json_data = json.loads(df.reset_index().to_json(orient='records'))
    coll.insert(json_data)
    print(" Save data to stock_info_tushare collection， OK")


def QA_SU_save_trade_date_all(client=DATABASE):
    data = QA_fetch_get_trade_date('', '')
    coll = client.trade_date
    coll.insert_many(data)


def QA_SU_save_stock_info(client=DATABASE):
    data = QA_fetch_get_stock_info('all')
    coll = client.stock_info
    coll.insert_many(data)


def QA_save_stock_day_all_bfq(client=DATABASE):
    df = ts.get_stock_basics()

    __coll = client.stock_day_bfq
    __coll.ensure_index('code')

    def saving_work(i):
        QA_util_log_info('Now Saving ==== %s' % (i))
        try:
            data_json = QA_fetch_get_stock_day(
                i, start='1990-01-01', if_fq='00')

            __coll.insert_many(data_json)
        except:
            QA_util_log_info('error in saving ==== %s' % str(i))

    for i_ in range(len(df.index)):
        QA_util_log_info('The %s of Total %s' % (i_, len(df.index)))
        QA_util_log_info('DOWNLOAD PROGRESS %s ' % str(
            float(i_ / len(df.index) * 100))[0:4] + '%')
        saving_work(df.index[i_])

    saving_work('hs300')
    saving_work('sz50')


def QA_save_stock_day_with_fqfactor(client=DATABASE):
    df = ts.get_stock_basics()

    __coll = client.stock_day
    __coll.ensure_index('code')

    def saving_work(i):
        QA_util_log_info('Now Saving ==== %s' % (i))
        try:
            data_hfq = QA_fetch_get_stock_day(
                i, start='1990-01-01', if_fq='02', type_='pd')
            data_json = QA_util_to_json_from_pandas(data_hfq)
            __coll.insert_many(data_json)
        except:
            QA_util_log_info('error in saving ==== %s' % str(i))
    for i_ in range(len(df.index)):
        QA_util_log_info('The %s of Total %s' % (i_, len(df.index)))
        QA_util_log_info('DOWNLOAD PROGRESS %s ' % str(
            float(i_ / len(df.index) * 100))[0:4] + '%')
        saving_work(df.index[i_])

    saving_work('hs300')
    saving_work('sz50')

    QA_util_log_info('Saving Process has been done !')
    return 0


def QA_save_lhb(client=DATABASE):
    __coll = client.lhb
    __coll.ensure_index('code')

    start = datetime.datetime.strptime("2006-07-01", "%Y-%m-%d").date()
    end = datetime.date.today()
    i = 0
    while start < end:
        i = i + 1
        start = start + datetime.timedelta(days=1)
        try:
            pd = QA_fetch_get_lhb(start.isoformat())
            if pd is None:
                continue
            data = pd\
                .assign(pchange=pd.pchange.apply(float))\
                .assign(amount=pd.amount.apply(float))\
                .assign(bratio=pd.bratio.apply(float))\
                .assign(sratio=pd.sratio.apply(float))\
                .assign(buy=pd.buy.apply(float))\
                .assign(sell=pd.sell.apply(float))
            # __coll.insert_many(QA_util_to_json_from_pandas(data))
            for i in range(0, len(data)):
                __coll.update({"code": data.iloc[i]['code'], "date": data.iloc[i]['date']}, {
                              "$set": QA_util_to_json_from_pandas(data)[i]}, upsert=True)
            time.sleep(2)
            if i % 10 == 0:
                time.sleep(60)
        except Exception as e:
            print("error codes:")
            time.sleep(2)
            continue


if __name__ == '__main__':
    QA_save_lhb()
