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


def generate_financial_df(code, start, end):
    # 1 retrieve financial date from db
    #df = QA_fetch_financial_report_adv(code,'1990-01-01', str(datetime.date.today())).data
    df = QA_fetch_financial_report_adv(code,'1990-01-01', '2018-09-05').data
    df_fin = pd.DataFrame(index=df.index)

    # 2 collect useful columns into new df
    # 1.每股指标
    df_fin['001基本每股收益'] = df.filter(regex=("001基本每股收益"))
    df_fin['002扣除非经常性损益每股收益'] = df.filter(regex=("002扣除非经常性损益每股收益"))
    df_fin['004每股净资产'] = df.filter(regex=("004每股净资产"))
    df_fin['006净资产收益率'] = df.filter(regex=("006净资产收益率"))
    df_fin['007每股经营现金流量'] = df.filter(regex=("007每股经营现金流量"))

    # 季度
    df_fin['230季度营业收入'] = df.filter(regex=("230营业收入"))
    df_fin['232季度归属于母公司所有者的净利润'] = df.filter(regex=("232归属于母公司所有者的净利润"))
    df_fin['233季度扣除非经常性损益后的净利润'] = df.filter(regex=("233扣除非经常性损益后的净利润"))
    df_fin['234季度经营活动产生的现金流量净额'] = df.filter(regex=("234经营活动产生的现金流量净额")) # 季度经营性净现金流 
    df_fin['235季度投资活动产生的现金流量净额'] = df.filter(regex=("235投资活动产生的现金流量净额"))
    df_fin['236季度筹资活动产生的现金流量净额'] = df.filter(regex=("236筹资活动产生的现金流量净额"))
    df_fin['237季度现金及现金等价物净增加额'] = df.filter(regex=("237现金及现金等价物净增加额"))     # 季度净现金流 ， verify against 225每股现金流量净额(元) * failed: use 237季度现金及现金等价物净增加额
    # 2.2.2 非流动负债
    df_fin['040资产总计'] = df.filter(regex=("040资产总计")) # 净资产 = 040资产总计 - 063负债合计(quarterly)， 004每股净资产 = 净资产/238总股本; verify 040资产总计 is quartarly or ttm:passed: just use 004每股净资产
    df_fin['062非流动负债合计'] = df.filter(regex=("062非流动负债合计"))
    df_fin['063负债合计'] = df.filter(regex=("063负债合计"))
    # 3. 利润表
    df_fin['092四、利润总额'] = df.filter(regex=("092"))    # 税前利润总额
    df_fin['095五、净利润'] = df.filter(regex=("095"))
    #df_fin['276近一年净利润'] = df.filter(regex=("276近一年净利润"))
    # 4. 现金流量表
    df_fin['101经营活动现金流入小计'] = df.filter(regex=("101经营活动现金流入小计"))                # verify against 007每股经营现金流量 & 219每股经营性现金流 passed: 
    df_fin['107经营活动产生的现金流量净额'] = df.filter(regex=("107经营活动产生的现金流量净额"))

    # 6. 经营效率分析
    df_fin['175总资产周转率'] = df.filter(regex=("175总资产周转率"))
    df_fin['176固定资产周转率'] = df.filter(regex=("176固定资产周转率"))
    df_fin['179流动资产周转率'] = df.filter(regex=("179流动资产周转率"))
    # 7. 发展能力分析
    df_fin['183营业收入增长率'] = df.filter(regex=("183营业收入增长率"))
    df_fin['184净利润增长率'] = df.filter(regex=("184净利润增长率"))
    df_fin['185净资产增长率'] = df.filter(regex=("185净资产增长率"))
    df_fin['189营业利润增长率'] = df.filter(regex=("189营业利润增长率"))
    # 8. 获利能力分析
    df_fin['194营业利润率'] = df.filter(regex=("194营业利润率"))
    # df_fin['197净资产收益率'] = df.filter(regex=("197净资产收益率"))   # duplicated with 006净资产收益率
    df_fin['201净利润率'] = df.filter(regex=("201净利润率"))
    df_fin['202销售毛利率'] = df.filter(regex=("202销售毛利率"))                                 # quarterly
    #df_fin['206扣除非经常性损益后的净利润'] = df.filter(regex=("206扣除非经常性损益后的净利润"))

    # 10. 现金流量分析
    # df_fin['219每股经营性现金流'] = df.filter(regex=("219每股经营性现金流")) # duplicated with 007每股经营现金流量
    df_fin['220营业收入现金含量'] = df.filter(regex=("220营业收入现金含量"))
    df_fin['225每股现金流量净额'] = df.filter(regex=("225每股现金流量净额"))
    df_fin['226经营净现金比率短期债务'] = df.filter(regex=("226经营净现金比率"))
    df_fin['227经营净现金比率全部债务'] = df.filter(regex=("227经营净现金比率"))
    df_fin['228经营活动现金净流量与净利润比率'] = df.filter(regex=("228经营活动现金净流量与净利润比率"))
    # 12.股本股东
    df_fin['238总股本'] = df.filter(regex=("238总股本"))
    df_fin['239已上市流通A股'] = df.filter(regex=("239已上市流通A股"))

    # 3 fillna first
    # record this columns for records
    nan_cols = df_fin.columns[df_fin.isnull().any()].tolist()
    #nan_cols

    # 002扣除非经常性损益每股收益 fillna using 001基本每股收益
    if '001基本每股收益' in nan_cols:
        print("Error: missing 001基本每股收益")
        nan_cols.remove('001基本每股收益')
    if '002扣除非经常性损益每股收益' in nan_cols:
        df_fin['002扣除非经常性损益每股收益'].fillna(value=df_fin['001基本每股收益'], inplace=True)
        nan_cols.remove('002扣除非经常性损益每股收益')

    while nan_cols:
        col = nan_cols.pop()
        if '季度' in col:
            df_fin[col].fillna(value=0, inplace=True)
        else:
            df_fin[col].fillna(method='bfill', inplace=True)
        #print(col)

    # 4 align index and remove discontinue quarters
    start_date = df_fin.index[0]
    end_date = df_fin.index[-1]
    rng_index = pd.date_range(start=start_date, end=end_date, freq='Q')

    # reindex using rng
    #df_fin = df_fin.loc[rng_index]
    df_fin = df_fin.reindex(rng_index)
    na_row_list = pd.isnull(df_fin).any(1).nonzero()[0] # any(1) check each row, without 1, it will return column result

    if na_row_list[-1] < len(df_fin)/2:
        df_fin.drop(df_fin.index[:(na_row_list[-1]+2)], inplace=True) # +2: after reindex, the first non-na season should be removed(which is the year)

    # 5 calculate some values
    if df_fin.index[0].quarter != 1:
        print("error, should drop off to start from 1st quarter")
        while df_fin.index[0].quarter != 1:
            df_fin.drop(df_fin.index[0], inplace=True)

    df_fin = reduce_to_quarter_value(df_fin, '101经营活动现金流入小计', '季度经营活动产生的现金流量')
    df_fin = reduce_to_quarter_value(df_fin, '006净资产收益率', '季度净资产收益率')
    df_fin = reduce_to_quarter_value(df_fin, '175总资产周转率', '季度总资产周转率')
    df_fin = reduce_to_quarter_value(df_fin, '176固定资产周转率', '季度固定资产周转率')
    df_fin = reduce_to_quarter_value(df_fin, '179流动资产周转率', '季度流动资产周转率')
    df_fin = reduce_to_quarter_value(df_fin, '095五、净利润', '季度净利润')
    df_fin = reduce_to_quarter_value(df_fin, '092四、利润总额', '季度税前净利润')
    # recalculate quarter growth rate
    df_fin = generate_quarter_growth_rate(df_fin, '季度净利润', '净利润增长率')
    df_fin = generate_quarter_growth_rate(df_fin, '232季度归属于母公司所有者的净利润', '归母公司净利润增长率')
    df_fin = generate_quarter_growth_rate(df_fin, '233季度扣除非经常性损益后的净利润', '扣非净利润增长率')
    df_fin = generate_quarter_growth_rate(df_fin, '230季度营业收入', '营业收入增长率')
    df_fin = generate_quarter_growth_rate(df_fin, '季度经营活动产生的现金流量', '经营性现金流增长率_cal')
    df_fin = generate_quarter_growth_rate(df_fin, '007每股经营现金流量', '经营性现金流增长率_007')
    df_fin = generate_quarter_growth_rate(df_fin, '季度净资产收益率', '净资产收益率增长率')

    # recalculate
    df_fin['净利润ttm'] = df_fin['季度净利润'].rolling(4).sum()
    df_fin['税前净利润ttm'] = df_fin['季度税前净利润'].rolling(4).sum()
    df_fin['母公司净利润ttm'] = df_fin['232季度归属于母公司所有者的净利润'].rolling(4).sum()
    df_fin['扣非净利润ttm'] = df_fin['233季度扣除非经常性损益后的净利润'].rolling(4).sum()
    df_fin['营业收入ttm'] = df_fin['230季度营业收入'].rolling(4).sum()
    df_fin['经营性现金流净额ttm'] = df_fin['234季度经营活动产生的现金流量净额'].rolling(4).sum()
    df_fin['经营性现金流ttm'] = df_fin['季度经营活动产生的现金流量'].rolling(4).sum()
    df_fin['净现金流ttm'] = df_fin['237季度现金及现金等价物净增加额'].rolling(4).sum()
    df_fin['净资产收益率ttm'] = df_fin['季度净资产收益率'].rolling(4).sum()
    df_fin['总资产周转率ttm'] = df_fin['季度总资产周转率'].rolling(4).sum()
    df_fin['固定资产周转率ttm'] = df_fin['季度固定资产周转率'].rolling(4).sum()
    df_fin['流动资产周转率ttm'] = df_fin['季度流动资产周转率'].rolling(4).sum()

    df_fin['EPS'] = df_fin['净利润ttm']/df_fin['238总股本']
    df_fin['EPS_cut'] = df_fin['扣非净利润ttm']/df_fin['238总股本']
    df_fin['ROA_q'] = df_fin['季度税前净利润']/df_fin['040资产总计']
    df_fin['ROA_ttm'] = df_fin['ROA_q'].rolling(4).sum()
    df_fin['扣非净利润率_q'] = df_fin['233季度扣除非经常性损益后的净利润']/df_fin['季度净利润']*df_fin['201净利润率']
    df_fin['经营现金流比扣非净利润_q'] = df_fin['季度经营活动产生的现金流量']/df_fin['233季度扣除非经常性损益后的净利润']
    df_fin['fin_leverage_q'] = df_fin['040资产总计']/(df_fin['004每股净资产']*df_fin['238总股本'])
    df_fin['debitequityratio_q'] = df_fin['062非流动负债合计']/(df_fin['004每股净资产']*df_fin['238总股本'])
    #df_fin['净现金流比总市值_ttm'] = df_fin['净现金流ttm']/(df_fin['238总股本']*p_close)
    #df_fin['经营性现金流净额比总市值ttm'] = df_fin['经营性现金流净额ttm']/(df_fin['238总股本']*p_close)
    #df_fin['经营性现金流比总市值_ttm'] = df_fin['经营性现金流ttm']/(df_fin['238总股本']*p_close)
    df_fin['code'] = str(code)

    na_row_list_2 = pd.isnull(df_fin).any(1).nonzero()[0] # any(1) check each row, without 1, it will return column result
    if na_row_list_2[-1] < len(df_fin)/2:
        df_fin.drop(df_fin.index[:(na_row_list_2[-1]+1)], inplace=True)

    return df_fin



def generate_technical_df(code, df_fin, start, end ):
    """
    # maximum indicator use 480 days
    """
    # 1 get stock df
    #df_stock = QA_fetch_stock_day_ts_adv(code, end='2018-09-05')
    df_stock = QA_fetch_stock_day_ts_adv(code, start=start, end=end)
    if len(df_stock) == 0:
        return None
    
    # 2 calculate tech indicator before financial indicators
    df_bench = get_stock_benchmark_df(code, start, end)
    #df_bench = df_sh # use sh as default benchmark
    # do align benchmark first, then do add_pchg
    df_stock, _, _ = align_with_benchmark(df_stock, df_bench)
    df_bench = df_add_pchg(df_bench)
    df_stock = df_add_pchg(df_stock)
    df_stock = df_add_relative_strength(df_stock, df_bench)

    # pure tech indicators
    df_stock = indicator_weighted_return(df_stock, 20, 'wgt_return_1m')
    df_stock = indicator_weighted_return(df_stock, 60, 'wgt_return_3m')
    df_stock = indicator_weighted_return(df_stock, 120, 'wgt_return_6m')
    df_stock = indicator_weighted_return(df_stock, 240, 'wgt_return_12m')
    df_stock = indicator_exp_weighted_return(df_stock, 20, 1, 'exp_wgt_return_1m')
    df_stock = indicator_exp_weighted_return(df_stock, 60, 3, 'exp_wgt_return_3m')
    df_stock = indicator_exp_weighted_return(df_stock, 120, 6, 'exp_wgt_return_6m')
    df_stock = indicator_exp_weighted_return(df_stock, 240, 12, 'exp_wgt_return_12m')
    df_stock = indicator_return(df_stock, 20, 'return_1m')
    df_stock = indicator_return(df_stock, 60, 'return_3m')
    df_stock = indicator_return(df_stock, 120, 'return_6m')
    df_stock = indicator_return(df_stock, 240, 'return_12m')
    df_stock['EMA5'] = talib.EMA(df_stock['close'].values, timeperiod=5)
    df_stock['EMA10'] = talib.EMA(df_stock['close'].values, timeperiod=10)
    df_stock['EMA20'] = talib.EMA(df_stock['close'].values, timeperiod=20)
    df_stock['EMA40'] = talib.EMA(df_stock['close'].values, timeperiod=40)
    df_stock['EMA60'] = talib.EMA(df_stock['close'].values, timeperiod=60)
    df_stock['EMA120'] = talib.EMA(df_stock['close'].values, timeperiod=120)

    df_stock['STD_1m'] = talib.STDDEV(df_stock['p_chg'].values, timeperiod=20, nbdev=1)
    df_stock['STD_3m'] = talib.STDDEV(df_stock['p_chg'].values, timeperiod=60, nbdev=1)
    df_stock['STD_6m'] = talib.STDDEV(df_stock['p_chg'].values, timeperiod=120, nbdev=1)
    df_stock['STD_12m'] = talib.STDDEV(df_stock['p_chg'].values, timeperiod=240, nbdev=1)

    df_stock['RSI_20'] = talib.RSI(df_stock['close'].values, timeperiod=20)

    # depends on df_fin
    df_stock = indicator_turnover_daily(df_fin, df_stock, 'turnover_d', 'volume')
    to_s = df_stock.turnover_d
    df_stock['turn_1m'] = to_s.rolling(20).mean()
    df_stock['turn_3m'] = to_s.rolling(60).mean()
    df_stock['turn_6m'] = to_s.rolling(120).mean()
    df_stock['turn_12m'] = to_s.rolling(240).mean()
    df_stock['turn_24m'] = to_s.rolling(480).mean()
    df_stock['bias_turn_1m'] = df_stock['turn_1m']/df_stock['turn_24m'] 
    df_stock['bias_turn_3m'] = df_stock['turn_3m']/df_stock['turn_24m'] 
    df_stock['bias_turn_6m'] = df_stock['turn_6m']/df_stock['turn_24m'] 

    df_indicator = indicator_weighted_return(df_stock, 20, 'wgt_return_1m')
    df_indicator = indicator_weighted_return(df_stock, 60, 'wgt_return_3m')
    df_indicator = indicator_weighted_return(df_stock, 120, 'wgt_return_6m')
    df_indicator = indicator_weighted_return(df_stock, 240, 'wgt_return_12m')
    df_indicator = indicator_exp_weighted_return(df_stock, 20, 1, 'exp_wgt_return_1m')
    df_indicator = indicator_exp_weighted_return(df_stock, 60, 3, 'exp_wgt_return_3m')
    df_indicator = indicator_exp_weighted_return(df_stock, 120, 6, 'exp_wgt_return_6m')
    df_indicator = indicator_exp_weighted_return(df_stock, 240, 12, 'exp_wgt_return_12m')

    # 3 trim off stock dates earlier than the first quarter date
    quater_start = df_fin.index[0]
    quater_end = df_fin.index[-1] + pd.offsets.QuarterEnd() # need to get next quarter's date
    df_stock = df_stock[(df_stock.index - pd.offsets.QuarterEnd() >= quater_start) & (df_stock.index + pd.offsets.QuarterEnd() <= quater_end)]

    # 4 generate financial indicators
    #df_indicator = pd.DataFrame(index=df_stock.index)
    df_indicator = df_stock

    df_indicator = indicator_PX_pershare(df_fin, df_stock, df_indicator, 'PE', 'close', 'EPS')
    df_indicator = indicator_PX_pershare(df_fin, df_stock, df_indicator, 'PE_cut', 'close', 'EPS_cut')
    df_indicator = indicator_PX_pershare(df_fin, df_stock, df_indicator, 'PB', 'close', '004每股净资产')
    df_indicator = indicator_PX(df_fin, df_stock, df_indicator, 'PS', 'close', '营业收入ttm')
    df_indicator = indicator_PX(df_fin, df_stock, df_indicator, 'PNCF', 'close', '净现金流ttm') 
    df_indicator = indicator_PX(df_fin, df_stock, df_indicator, 'POCF', 'close', '经营性现金流ttm')
    df_indicator = indicator_PX_pershare(df_fin, df_indicator, df_indicator, 'PEG', 'PE', '净利润增长率')
    df_indicator = indicator_PX_pershare(df_fin, df_indicator, df_indicator, 'PEG_cut', 'PE_cut', '扣非净利润增长率')
    df_indicator['sales_G_q'] = quarterly_to_daily(df_fin, df_stock, df_indicator, 'sales_G_q', '营业收入增长率')
    df_indicator['profit_G_q'] = quarterly_to_daily(df_fin, df_stock, df_indicator, 'profit_G_q', '净利润增长率')
    df_indicator['profit_cut_G_q'] = quarterly_to_daily(df_fin, df_stock, df_indicator, 'profit_cut_G_q', '扣非净利润增长率')
    df_indicator['OCF_cal_G_q'] = quarterly_to_daily(df_fin, df_stock, df_indicator, 'OCF_cal_G_q', '经营性现金流增长率_cal')
    df_indicator['OCF_G_q'] = quarterly_to_daily(df_fin, df_stock, df_indicator, 'OCF_G_q', '经营性现金流增长率_007')
    df_indicator['ROE_G_q'] = quarterly_to_daily(df_fin, df_stock, df_indicator, 'ROE_G_q', '净资产收益率增长率')
    df_indicator['ROE_q'] = quarterly_to_daily(df_fin, df_stock, df_indicator, 'ROE_q', '季度净资产收益率')
    df_indicator['ROA_q'] = quarterly_to_daily(df_fin, df_stock, df_indicator, 'ROA_q', 'ROA_q')
    df_indicator['grossprofit_q'] = quarterly_to_daily(df_fin, df_stock, df_indicator, 'grossprofit_q', '202销售毛利率')
    df_indicator['profitmargin_q'] = quarterly_to_daily(df_fin, df_stock, df_indicator, 'profitmargin_q', '扣非净利润率_q')
    df_indicator['assetturnover_q'] = quarterly_to_daily(df_fin, df_stock, df_indicator, 'assetturnover_q', '季度总资产周转率')
    df_indicator['OCF_netprofit_r_q'] = quarterly_to_daily(df_fin, df_stock, df_indicator, 'OCF_netprofit_r_q', '经营现金流比扣非净利润_q')
    df_indicator['fin_leverage_q'] = quarterly_to_daily(df_fin, df_stock, df_indicator, 'fin_leverage_q', 'fin_leverage_q') 
    df_indicator['debitequityratio_q'] = quarterly_to_daily(df_fin, df_stock, df_indicator, 'debitequityratio_q', 'debitequityratio_q') 
    df_indicator = total_capital(df_fin, df_stock, df_indicator, 'captital_tot')

    return df_indicator

def QA_SU_save_indicator_2_db(stock_list, start=None, end=None, client=DATABASE):
    """
    1. this function only do first time save 2 dfs
    2. appending work will be another function
    """
    coll_fin_indicator = client.df_financial_indicator
    coll_fin_indicator.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)])
    coll_tech_indicator = client.df_tech_indicator
    coll_tech_indicator.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)])

    #logger = multiprocessing.log_to_stderr()
    #logger.setLevel(multiprocessing.SUBDEBUG)
    #logging.basicConfig(level=logging.DEBUG)

    # get 240 back from start date ( in order to calculate 480 days back indicators)
    df_bench = get_stock_benchmark_df(code)
    if start == None:
        start = '2004-01-01'
    else:
        """
        start_index = df_bench.loc[start].name
        start_480_index = df_bench.iloc[df_bench.index.get_loc(start_index) - 479].name
        start = str(start_480_index)[:10]
        """
        start = get_backward_xdays(start, 480)

    if end == None:
        end = str(datetime.date.today())

    err = []
    def _saving_work(code, coll_fin_indicator, coll_tech_indicator):
        try: 
            #logging.debug("process {} now working on stock {}".format(os.getpid(), code))
            QA_util_log_info('Now Processing ==== %s' % (code))
            df_fin = generate_financial_df(code)
            #logging.debug("generated df_fin:")
            #logging.debug(df_fin.head())
            if len(df_fin) > 0:
                df_fin['date'] = df_fin.index.strftime('%Y-%m-%d')
                df_fin['date_stamp'] = df_fin['date'].apply(lambda x: QA_util_date_stamp(x))
                fin_json = QA_util_to_json_from_pandas(df_fin)
                coll_fin_indicator.insert_many(fin_json)
                #logging.debug("df_fin successfully inserted into DB")

            df_indicator = generate_technical_df(code, df_fin, start, end)
            #logging.debug("generated df_indicator:")
            #logging.debug(df_indicator.head())
            if len(df_indicator) > 0:
                df_indicator['date'] = df_indicator.index.strftime('%Y-%m-%d')
                df_indicator['date_stamp'] = df_indicator['date'].apply(lambda x: QA_util_date_stamp(x))
                indicator_json = QA_util_to_json_from_pandas(df_indicator)
                coll_tech_indicator.insert_many(indicator_json)
                #logging.debug("df_indicator successfully inserted into DB")
        except Exception as e:
            QA_util_log_info('error in processing ==== %s' % str(code))
            err.append(code)
            print("The exception is {}".format(str(e)))

    executor = ThreadPoolExecutor(max_workers=1)
    res = {executor.submit(_saving_work, stock_list[i], coll_fin_indicator, coll_tech_indicator) for i in range(len(stock_list))}

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

def norm_rank(ss):
    ss_rank = ss.rank()
    ss_rank_norm = ss_rank/ss_rank.max()
    return ss_rank_norm

def QA_SU_normalized_tech_indicator(stock_list, indicator_key, start=None, end=None, client=DATABASE):
    #start_date = '2004-01-01'
    #end_date = '2018-09-05'
    df_sh = QA_fetch_index_day_ts_adv('000001', start=start, end=end)
    df_indicator = pd.DataFrame(index=df_sh.index)
    for stock_code in stock_list:
        df_tech = QA_fetch_stock_tech_indicator_adv(stock_code, start, end, keys=indicator_key)
        if df_tech is not None:
            df_tech.columns = [stock_code]
            df_indicator = pd.concat([df_indicator, df_tech], axis=1, join_axes=[df_indicator.index])

    df_indicator_rank = df_indicator.apply(norm_rank, axis=1)

    coll_norm_indicator = client.df_tech_indicator_normalized
    coll_norm_indicator.create_index([("key", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)])
    try:
        if len(df_indicator_rank) > 0:
            df_indicator_rank['key'] = str(indicator_key)
            df_indicator_rank['date'] = df_indicator_rank.index.strftime('%Y-%m-%d')
            df_indicator_rank['date_stamp'] = df_indicator_rank['date'].apply(lambda x: QA_util_date_stamp(x))
            tech_json = QA_util_to_json_from_pandas(df_indicator_rank)
            coll_norm_indicator.insert_many(tech_json)
    except Exception as e:
        QA_util_log_info('error in processing indicator ==== %s' % str(indicator_key))
        print("The exception is {}".format(str(e)))

    QA_util_log_info('successfully processed indicator ==== %s' % str(indicator_key))

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

def QA_SU_calculate_tech_indicator_cols(code, start='all', end=None):
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

    df_indicator['return_1m'] = indicator_return(df_stock_20, 20)
    df_indicator['return_3m'] = indicator_return(df_stock_60, 60)
    df_indicator['return_6m'] = indicator_return(df_stock_120, 120)
    df_indicator['return_12m'] = indicator_return(df_stock_240, 240)

    return df_indicator


    

def QA_util_drop_colume_all(coll, columns, db=DATABASE):
    _coll = db[coll]
    for col in columns:
        _coll.update({}, {'$unset': {col: 1}}, multi=True)

def QA_util_drop_colume(coll, code, columns, db=DATABASE):
    _coll = db[coll]
    for col in columns:
        _coll.update({'code': code}, {'$unset': {col: 1}}, multi=True)

def QA_util_drop_rows(coll, code, start_date, end_date, db=DATABASE):
    _coll = db[coll]
    query = {'code': code, 'date': {'$gte': start_date, '$lte': end_date}}
    result = _coll.delete_many(query)
    return result.deleted_count

def QA_util_insert_colume(coll, df):
    for i in range(len(df)):
        coll.update({'code': df.iloc[i, df.columns.get_loc('code')], 'date': df.iloc[i, df.columns.get_loc('date')]}, 
            {'$set': {'return_1m': df.iloc[i, df.columns.get_loc('return_1m')], 
            'return_3m': df.iloc[i, df.columns.get_loc('return_3m')],
            'return_6m': df.iloc[i, df.columns.get_loc('return_6m')],
            'return_12m': df.iloc[i, df.columns.get_loc('return_12m')]}})

def recalculate_insert_returnxm(stock_list, db=DATABASE):
    # 1. collection to use
    coll_stock_tech_indicator = db.stock_tech_indicator_3
    #coll_stock_tech_indicator.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)])

    err = []
    def _doing_work(code, coll_stock_tech_indicator):
        try: 
            #logging.debug("process {} now working on stock {}".format(os.getpid(), code))
            QA_util_log_info('Now Processing ==== %s' % (code))

            ref = coll_stock_tech_indicator.find({'code': str(code)[:6]})

            if ref.count() > 0:
                start_date = ref[0]['date']
                end_date = ref[ref.count() - 1]['date']

                if start_date != end_date:
                    df_indicator = QA_SU_calculate_tech_indicator_cols(code, start_date, end_date)
            
                    if len(df_indicator) > 0:
                        if len(df_indicator) != ref.count():
                            raise Exception('df len is not equal to ref count')
                        df_indicator['date'] = df_indicator.index.strftime('%Y-%m-%d')
                        QA_util_insert_colume(coll_stock_tech_indicator, df_indicator)

                        #logging.debug("df_indicator successfully inserted into DB")
        except Exception as e:
            QA_util_log_info('Error in processing ==== %s' % str(code))
            err.append(code)
            print("The exception is {}".format(str(e)))
    
    """
    for code in stock_list:
        _doing_work(code, coll_stock_tech_indicator)
    """

    executor = ThreadPoolExecutor(max_workers=1)
    res = {executor.submit(_doing_work, stock_list[i], coll_stock_tech_indicator) for i in range(len(stock_list))}

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

#def QA_util_append_rows(start, end, db=DATABASE):

if __name__ == '__main__':
    stock_list = pickle.load(open("D:/abu/data/download/tdx/stock_list.pkl", 'rb'))
    #index1 = int(len(stock_list)/10)
    #len2 = int(len(stock_list)/20)
    #stock_list = stock_list[:index1]
    #stock_list = stock_list[index1:index1+len2]
    #stock_list = stock_list[index1+17*len2:]
    #stock_list = ['000693', '000333', '603713', '603920', '601518', '600401', '002070', '600680', '600769', '600773', '600774', '600775', '600776', '600777', '600778', '600779', '600780', '600781', '600782', '600783', '600784', '600785', '600787', '600789', '600790', '600791', '600792', '600794', '600795', '600796', '600797', '601188', '601200', '601238', '601518', '603713', '603920']

    """
    pool = Pool(6) # 8 core
    split = 6
    split_len = int(len(stock_list)/split)
    for i in range(split):
        #QA_util_log_info('The %s of Total %s' % (i+1, len(stock_list)))
        #QA_util_log_info('Processing Progress %s ' % str(float((i+1) / len(stock_list) * 100))[0:4] + '%')

        #_saving_work(stock_list[i], coll_fin_indicator, coll_tech_indicator)
        if i == (split-1):
            pool.apply_async(QA_SU_save_indicator_2_db, args=(stock_list[i*split_len:], ))
        else:
            pool.apply_async(QA_SU_save_indicator_2_db, args=(stock_list[i*split_len:(i+1)*split_len], ))

    pool.close()
    pool.join()
    """

    """
    #indicator_list = QA_fetch_stock_tech_indicator_adv('000001').columns
    indicator_list = [ 'OCF_G_q', 'OCF_cal_G_q', 'OCF_netprofit_r_q', 'PB', 'PE', 'PEG', 'PEG_cut',
       'PE_cut', 'PNCF', 'POCF', 'PS', 'ROA_q', 'ROE_G_q', 'ROE_q', 'RSI_20',
       'STD_12m', 'STD_1m', 'STD_3m', 'STD_6m', 'assetturnover_q',
       'bias_turn_1m', 'bias_turn_3m', 'bias_turn_6m', 'captital_tot', 
       'debitequityratio_q', 'exp_wgt_return_12m', 'exp_wgt_return_1m', 'exp_wgt_return_3m',
       'exp_wgt_return_6m', 'fin_leverage_q', 'grossprofit_q', 'profit_G_q', 'profit_cut_G_q',
       'profitmargin_q', 'return_12m', 'return_1m', 'return_3m', 'return_6m',
       'rs', 'sales_G_q', 'turn_12m', 'turn_1m', 'turn_24m', 'turn_3m',
       'turn_6m', 'turnover_d',  'wgt_return_12m', 'wgt_return_1m',
       'wgt_return_3m', 'wgt_return_6m']
       
    pool = Pool(6) # 8 core
    split = 6
    split_len = int(len(stock_list)/split)
    for i, key in enumerate(indicator_list):
        QA_util_log_info('The %s of Total %s' % (i+1, len(indicator_list)))
        QA_util_log_info('Processing Progress %s ' % str(float((i+1) / len(indicator_list) * 100))[0:4] + '%')

        pool.apply_async(QA_SU_normalized_tech_indicator, args=(stock_list, key))


    pool.close()
    pool.join()
    """

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


    #QA_SU_append_stock_tech_indicator(['002460']) # 002460
    """
    coll = 'stock_tech_indicator_3'
    code = '002460'
    start_date = '2018-09-17'
    end_date = str(datetime.date.today())
    num_del = QA_util_drop_rows(coll, code, start_date, end_date)
    print(num_del)
    """

    """
    coll = 'lhb'
    cols = ['bratio']
    QA_util_drop_colume(coll, cols)
    """
    ######################################
    # fix return_xm misalignment issues
    ######################################
    # 1 drop columns: 
    """
    cols = ['return_1m', 'return_3m', 'return_6m', 'return_12m']
    coll = 'stock_tech_indicator_3'
    QA_util_drop_colume(coll, cols)
    
    # 2. calculate new columns
    pool = Pool(7) # 8 core
    split = 7
    split_len = int(len(stock_list)/split)
    for i in range(split):
        #QA_util_log_info('The %s of Total %s' % (i+1, len(stock_list)))
        #QA_util_log_info('Processing Progress %s ' % str(float((i+1) / len(stock_list) * 100))[0:4] + '%')

        #_saving_work(stock_list[i], coll_fin_indicator, coll_tech_indicator)
        if i == (split-1):
            pool.apply_async(recalculate_insert_returnxm, args=(stock_list[i*split_len:], ))
        else:
            pool.apply_async(recalculate_insert_returnxm, args=(stock_list[i*split_len:(i+1)*split_len], ))

    pool.close()
    pool.join()
    """

