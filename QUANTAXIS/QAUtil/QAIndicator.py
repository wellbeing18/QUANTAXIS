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

from QUANTAXIS.QAFetch.QAQuery_Advance import (QA_fetch_index_day_ts_adv)


df_sh = QA_fetch_index_day_ts_adv('000001')
#df_sz = QA_fetch_index_day_ts_adv('399001')
#df_zx = QA_fetch_index_day_ts_adv('399005')
#df_cy = QA_fetch_index_day_ts_adv('399006')

def get_next_valid_date(df, given_date_str):
    datetime_ = pd.to_datetime(given_date_str)
    return df.iloc[df.index.get_loc(datetime_, method='nearest')].name

def get_backward_xdays(start, days):
    df_bench = get_stock_benchmark_df()
    start_index = df_bench.loc[get_next_valid_date(df_bench, start)].name
    start_index_num = df_bench.index.get_loc(start_index) - days
    # make sure the backward index is not over the boundary(the first date of bench)
    if start_index_num < 0:
        start_index_num = 0
    start_backward_index = df_bench.iloc[start_index_num].name
    backward_start = str(start_backward_index)[:10]
    return backward_start

def reduce_to_quarter_value(df, from_col, to_col):
    for index, row in df.iterrows():
        if index.quarter == 1:
            df.at[index, to_col] = df.at[index, from_col]
        else:
            df.at[index, to_col] = df.at[index, from_col] - df.at[get_previous_quarter_datetime(index), from_col]
    return df

def generate_quarter_growth_rate(df, from_col, to_col):
    for index, row in df.iterrows():
        if index - pd.DateOffset(years=1) >= df.index[0] and (df.at[index - pd.DateOffset(years=1) , from_col]):
            df.at[index, to_col] = (df.at[index, from_col] - df.at[index - pd.DateOffset(years=1) , from_col])/abs(df.at[index - pd.DateOffset(years=1) , from_col]) * 100
    return df

# relative strenght
def relative_strength(df_a, df_b, key='p_chg', N=60):
    # return: rs: relative strength series with len N
    #         sum_rs: sum of rs
    # assumption: 
    # df_a & df_b are aligned in index
    rs = df_a.ix[-N:,key] - df_b.ix[-N:,key]
    sum_rs = sum(rs)
    return rs, sum_rs
# instead of using .ix, can use .loc or iloc
# df_a.loc[df_a.index[-N:], key]
# df_a.iloc[-N:, df_a.columns.get_loc(key)]

def df_add_relative_strength(df_a, df_b, key='p_chg'):
    # assumption: 
    # df_a & df_b are aligned in index
    #df_a.loc[:, 'rs'] = df_a.loc[:, key] - df_b.loc[df_a.index, key]
    #return df_a
    return df_a.loc[:, key] - df_b.loc[df_a.index, key]

def df_add_pchg(df):
    # add colume 'p_chg' to df
    # will remove 1st row by default
    df['pre_close'] = df['close'].shift(1)
    df = df.iloc[1:, :]
    df.loc[:, 'p_chg'] = (df.loc[:, 'close'] - df.loc[:, 'pre_close'])/df.loc[:, 'pre_close']*100
    return df
#df_test = df_test.assign(p_chg=(df_test['close'] - df_test['pre_close'])/df_test['pre_close']*100)
#df_test.head()

def align_with_benchmark(df, df_bench):
    """
    1. align df against df_bench, missed index of df will be NAN
    2. fillna
    return: df_align start/end date same as original df
    """
    # get some status before conversion
    same_end = df.index[-1] == df_bench.index[-1]
    same_start = df.index[0] == df_bench.index[0]
    # the benchmark df's index as ref to create a new df with start date same as df.index
    df_align = df.loc[df_bench.index]
    # drop some columns
    df_align.drop(['date'], axis=1, inplace=True) 
    # fillna, first volume na to 0
    df_align.volume.fillna(value=0, inplace=True)
    # fill close & code
    df_align.close.fillna(method='ffill', inplace=True)
    df_align.close.fillna(method='bfill', inplace=True)
    df_align.code.fillna(method='ffill', inplace=True)
    df_align.code.fillna(method='bfill', inplace=True)
    # all other columns use close to fill
    df_align.open.fillna(value=df_align.close, inplace=True)
    df_align.close.fillna(value=df_align.close, inplace=True)
    df_align.high.fillna(value=df_align.close, inplace=True)
    df_align.low.fillna(value=df_align.close, inplace=True)
    df_align = df_align.iloc[df_align['volume'].nonzero()[0][0]:]
    return df_align, same_end, same_start



def get_stock_benchmark_df(stock_code=None, start=None, end=None):
    """
    if stock_code[:2] in ['60']:
        df_bench = df_sh
    elif stock_code[:3] in ['000']:
        df_bench = df_sz
    elif stock_code[:3] in ['002']:
        df_bench = df_zx
    elif stock_code[:3] in ['300']:
        df_bench = df_cy
    else:
        df_bench = df_sh
    return df_bench
    """
    if start == None and end == None:
        #df_sh = QA_fetch_index_day_ts_adv('000001')
        return df_sh # use shang zheng as unified benchmark
    elif start != None and end != None:
        return QA_fetch_index_day_ts_adv('000001', start, end)

def get_previous_quarter_datetime(datetime_idx):
    if str(datetime_idx)[5:10] in ['03-31', '06-30', '09-30', '12-31']:
        return datetime_idx
    else:
        return datetime_idx - pd.offsets.QuarterEnd()

def indicator_turnover_daily(df_fin, df_stock, PX, P):
    for index, row in df_stock.iterrows():
        fin_idx = get_previous_quarter_datetime(index)
        try:
            df_stock.at[index, PX] = df_stock.at[index, P]*100/df_fin.at[fin_idx, '239已上市流通A股'] # 1手 100 股
        except KeyError as e:
            #print('catch key error at fin_idx: {}'.format(fin_idx))
            df_stock.at[index, PX] = df_stock.at[index, P]*100/df_fin.at[df_fin.index[0], '239已上市流通A股']
    return df_stock

def indicator_return(df, days):#, key):
    # add colume 'p_chg' to df
    # will remove 1st row by default
    ts_shift = df['close'].shift(days)
    ts_close = df.iloc[days:, df.columns.get_loc('close')]
    #df.loc[:, key] = (ts_close - ts_shift)/ts_shift*100
    #return df
    return (ts_close - ts_shift)/ts_shift*100

def indicator_weighted_return(df, days, key):
    for index, row in df.iloc[days:,:].iterrows():
    #for index, row in df.iloc[-1:,:].iterrows():
        #print(index)
        ts_turn = df.iloc[df.index.get_loc(index)-days+1:df.index.get_loc(index)+1, df.columns.get_loc('turnover_d')]
        #print(ts_20_turn)
        #print(len(ts_20_turn))
        ts_turn = ts_turn/ts_turn.sum()
        ts_return = df.iloc[df.index.get_loc(index)-days+1:df.index.get_loc(index)+1, df.columns.get_loc('p_chg')]
        #print(ts_20_return)
        df.at[index, key] = (ts_turn*ts_return).sum()
    return df

def indicator_exp_weighted_return(df, days, n_month, key):
    xi = np.array(range(days-1, -1, -1))
    exp_arr = np.exp(-xi/n_month/4) 
    for index, row in df.iloc[days:,:].iterrows():
        ts_turn = df.iloc[df.index.get_loc(index)-days+1:df.index.get_loc(index)+1, df.columns.get_loc('turnover_d')]
        ts_turn = ts_turn/ts_turn.sum()
        ts_return = df.iloc[df.index.get_loc(index)-days+1:df.index.get_loc(index)+1, df.columns.get_loc('p_chg')]
        df.at[index, key] = (ts_turn*ts_return*exp_arr).sum()
    return df

def previous_quarter_enddate(timestamp):
    return get_previous_quarter_datetime(timestamp)

def indicator_PE(df_fin, df_stock, df_indicator):
    for index, row in df_stock.iterrows():
        df_indicator.at[index, 'PE'] = df_stock.at[index, 'close']/df_fin.at[get_previous_quarter_datetime(index), 'EPS']
    return df_indicator

def indicator_PX_pershare(df_fin, df_stock, df_indicator, PX, P, X):
    for index, row in df_stock.iterrows():
        df_indicator.at[index, PX] = df_stock.at[index, P]/df_fin.at[get_previous_quarter_datetime(index), X]
    return df_indicator

def indicator_PX(df_fin, df_stock, df_indicator, PX, P, X):
    for index, row in df_stock.iterrows():
        df_indicator.at[index, PX] = df_stock.at[index, P]*df_fin.at[get_previous_quarter_datetime(index), '238总股本']/df_fin.at[get_previous_quarter_datetime(index), X]
    return df_indicator

def quarterly_to_daily(df_fin, df_stock, df_indicator, to_col, from_col):
    for index, row in df_stock.iterrows():
        df_indicator.at[index, to_col] = df_fin.at[get_previous_quarter_datetime(index), from_col]
    return df_indicator

def total_capital(df_fin, df_stock, df_indicator, captital_tot):
    for index, row in df_stock.iterrows():
        df_indicator.at[index, captital_tot] = math.log10(df_stock.at[index, 'close']*df_fin.at[get_previous_quarter_datetime(index), '238总股本'])
    return df_indicator
