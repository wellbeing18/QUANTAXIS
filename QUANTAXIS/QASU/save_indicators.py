import json
import os
import datetime
from multiprocessing import Pool
import multiprocessing
import pymongo
import pickle
import pandas as pd
import numpy as np
import math
import talib
import concurrent
from concurrent.futures import ThreadPoolExecutor
import logging

from QUANTAXIS.QAUtil.QAIndicator import *
import warnings; warnings.simplefilter('ignore')
from QUANTAXIS.QAUtil.QASetting import DATABASE

from QUANTAXIS.QAFetch.QAQuery_Advance import (QA_fetch_financial_report_adv, QA_fetch_stock_day_ts_adv, 
                                                QA_fetch_index_day_ts_adv, QA_fetch_stock_tech_indicator_adv)
from QUANTAXIS.QAUtil import QA_util_date_stamp, QA_util_to_json_from_pandas, QA_util_log_info, QA_util_get_next_day


def QA_SU_calculate_stock_tech_indicator(code, start='all', end=None):
    # 2. handle start end dates
    if start == 'all':                     # create mode: start='all', end=None
        start = '1990-01-01'

    if end is None:
        end = str(datetime.date.today())

    # 3. get full stock and index df, align them
    df_stock = QA_fetch_stock_day_ts_adv(code)
    df_bench = get_stock_benchmark_df(code)
    # not necessary with same end( if stock didn't open on the last date, by default there is no trade data for the day)
    df_stock, _, _ = align_with_benchmark(df_stock, df_bench)
    # add p_chg
    df_stock = df_add_pchg(df_stock)
    df_bench = df_add_pchg(df_bench)
    # moving average indicators
    df_stock['EMA5'] = talib.EMA(df_stock['close'].values, timeperiod=5)
    df_stock['EMA10'] = talib.EMA(df_stock['close'].values, timeperiod=10)
    df_stock['EMA20'] = talib.EMA(df_stock['close'].values, timeperiod=20)
    df_stock['EMA40'] = talib.EMA(df_stock['close'].values, timeperiod=40)
    df_stock['EMA60'] = talib.EMA(df_stock['close'].values, timeperiod=60)
    df_stock['EMA120'] = talib.EMA(df_stock['close'].values, timeperiod=120)
    df_stock['RSI_20'] = talib.RSI(df_stock['close'].values, timeperiod=20)
    
    # 4. slice df_stock for preprocessing
    # since df_stock is aligned with df_bench, 
    #df_stock_480 = df_stock.loc[get_backward_xdays(start, 480):end]
    df_stock_240 = df_stock.loc[get_backward_xdays(start, 240):end]
    df_stock_120 = df_stock.loc[get_backward_xdays(start, 120):end]
    df_stock_60 = df_stock.loc[get_backward_xdays(start, 60):end]
    df_stock_20 = df_stock.loc[get_backward_xdays(start, 20):end]
    #df_stock_10 = df_stock.loc[get_backward_xdays(start, 10):end]
    #df_stock_5 = df_stock.loc[get_backward_xdays(start, 5):end]

    
    # 5. 
    #df_indicator = pd.DataFrame(index=df_bench.loc[start:end].index)
    df_indicator = df_stock.loc[start:end].copy() # deep copy slice
    df_indicator['RS'] = df_add_relative_strength(df_stock.loc[start:end], df_bench.loc[start:end])

    df_indicator['return_1m'] = indicator_return(df_stock_20, 20)
    df_indicator['return_3m'] = indicator_return(df_stock_60, 60)
    df_indicator['return_6m'] = indicator_return(df_stock_120, 120)
    df_indicator['return_12m'] = indicator_return(df_stock_240, 240)

    len_df = len(df_indicator)

    # talib EMA, RSI has running property, need use more days then required minimum
    # no impact with moving average
    df_indicator['STD_1m'] = talib.STDDEV(df_stock_20['p_chg'].values, timeperiod=20, nbdev=1)[-len_df:]
    df_indicator['STD_3m'] = talib.STDDEV(df_stock_60['p_chg'].values, timeperiod=60, nbdev=1)[-len_df:]
    df_indicator['STD_6m'] = talib.STDDEV(df_stock_120['p_chg'].values, timeperiod=120, nbdev=1)[-len_df:]
    df_indicator['STD_12m'] = talib.STDDEV(df_stock_240['p_chg'].values, timeperiod=240, nbdev=1)[-len_df:]

    return df_indicator

def QA_SU_append_stock_tech_indicator(stock_list, start='all', end=None, client=DATABASE):
    # 1. collection to use
    coll_stock_tech_indicator = client.stock_tech_indicator_3
    coll_stock_tech_indicator.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)])

    err = []
    def _saving_work(code, coll_stock_tech_indicator):
        try: 
            #logging.debug("process {} now working on stock {}".format(os.getpid(), code))
            QA_util_log_info('Now Processing ==== %s' % (code))

            ref = coll_stock_tech_indicator.find({'code': str(code)[:6]})
            end_date = str(datetime.date.today())

            if ref.count() > 0:
                start_date = ref[ref.count() - 1]['date']
                if start_date != end_date:
                    start_date = QA_util_get_next_day(start_date)
                    df_indicator = QA_SU_calculate_stock_tech_indicator(code, start_date, end_date)
            
                    if len(df_indicator) > 0:
                        df_indicator['date'] = df_indicator.index.strftime('%Y-%m-%d')
                        df_indicator['date_stamp'] = df_indicator['date'].apply(lambda x: QA_util_date_stamp(x))
                        indicator_json = QA_util_to_json_from_pandas(df_indicator)
                        coll_stock_tech_indicator.insert_many(indicator_json)
                        #logging.debug("df_indicator successfully inserted into DB")
            else:
                start_date = '1990-01-01'
                df_indicator = QA_SU_calculate_stock_tech_indicator(code, start_date, end_date)
                if len(df_indicator) > 0:
                    df_indicator['date'] = df_indicator.index.strftime('%Y-%m-%d')
                    df_indicator['date_stamp'] = df_indicator['date'].apply(lambda x: QA_util_date_stamp(x))
                    indicator_json = QA_util_to_json_from_pandas(df_indicator)
                    coll_stock_tech_indicator.insert_many(indicator_json)
                    #logging.debug("df_indicator successfully inserted into DB")

        except Exception as e:
            QA_util_log_info('error in processing ==== %s' % str(code))
            err.append(code)
            print("The exception is {}".format(str(e)))
    """
    for code in stock_list:
        _saving_work(code, coll_stock_tech_indicator)
    """
    executor = ThreadPoolExecutor(max_workers=1)
    res = {executor.submit(_saving_work, stock_list[i], coll_stock_tech_indicator) for i in range(len(stock_list))}

    count = 0
    for i_ in concurrent.futures.as_completed(res):
        QA_util_log_info('The {} of Total {}'.format(count+1, len(stock_list)))

        strProgress = 'Processing PROGRESS {} '.format(str(float((count+1) / len(stock_list) * 100))[0:4] + '%')
        intProgress = int((count+1) / len(stock_list) * 10000.0)
        QA_util_log_info(strProgress, ui_progress_int_value=intProgress)
        count = count + 1

    if len(err) < 1:
        QA_util_log_info('SUCCESS')
    else:
        QA_util_log_info(' ERROR CODE \n ')
        QA_util_log_info(err)  

def QA_SU_append_stock_pure_tech_indicator():
    stock_list = pickle.load(open("D:/abu/data/download/tdx/stock_list.pkl", 'rb'))
    pool = Pool(7) # 8 core
    split = 7
    split_len = int(len(stock_list)/split)
    for i in range(split):
        #QA_util_log_info('The %s of Total %s' % (i+1, len(stock_list)))
        #QA_util_log_info('Processing Progress %s ' % str(float((i+1) / len(stock_list) * 100))[0:4] + '%')

        #_saving_work(stock_list[i], coll_fin_indicator, coll_tech_indicator)
        if i == (split-1):
            pool.apply_async(QA_SU_append_stock_tech_indicator, args=(stock_list[i*split_len:], ))
        else:
            pool.apply_async(QA_SU_append_stock_tech_indicator, args=(stock_list[i*split_len:(i+1)*split_len], ))

    pool.close()
    pool.join()