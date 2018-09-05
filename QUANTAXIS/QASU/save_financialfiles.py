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
import os
import sys

import pymongo
import pickle
from QUANTAXIS.QAFetch.QAfinancial import (download_financialzip, parse_all,
                                           parse_filelist, QA_fecth_local_financial_report_cn)
from QUANTAXIS.QASetting.QALocalize import (cache_path, download_path, qa_path,
                                            setting_path)
from QUANTAXIS.QAUtil import DATABASE, QA_util_date_int2str, QA_util_log_info
from QUANTAXIS.QAUtil.QASql import ASCENDING, DESCENDING
from QUANTAXIS.QAUtil.QATransform import QA_util_to_json_from_pandas
import concurrent
from concurrent.futures import ThreadPoolExecutor

def QA_SU_save_financial_files():
    """本地存储financialdata
    """
    download_financialzip()
    coll = DATABASE.financial
    coll.create_index(
        [("code", ASCENDING), ("report_date", ASCENDING)], unique=True)
    for item in os.listdir(download_path):
        if item[0:4] != 'gpcw':
            print(
                "file ", item, " is not start with gpcw , seems not a financial file , ignore!")
            continue

        date = int(item.split('.')[0][-8:])
        print('QUANTAXIS NOW SAVING {}'.format(date))
        if coll.find({'report_date': date}).count() < 100:

            print(coll.find({'report_date': date}).count())
            data = QA_util_to_json_from_pandas(parse_filelist([item]).reset_index(
            ).drop_duplicates(subset=['code', 'report_date']).sort_index())
            try:
                coll.insert_many(data, ordered=False)

            except Exception as e:
                if isinstance(e, MemoryError):
                    coll.insert_many(data, ordered=True)
                elif isinstance(e, pymongo.bulk.BulkWriteError):
                    pass
        else:
            print('ALL READY IN DATABASE')

    print('SUCCESSFULLY SAVE/UPDATE FINANCIAL DATA')

def QA_SU_save_stock_financial():
    """
    save local financial files( from sina) to 3 tables
    """
    
    stock_list = pickle.load(open("C:/Users/ben_msi/abu/data/download/tdx/stock_list.pkl", 'rb'))
    #stock_list = ['000005']
    coll_balance = DATABASE.fin_balance_cn
    coll_balance.create_index([("code", ASCENDING)])
    coll_profit = DATABASE.fin_profit_cn
    coll_profit.create_index([("code", ASCENDING)])
    coll_cashflow = DATABASE.fin_cashflow_cn
    coll_cashflow.create_index([("code", ASCENDING)])

    err = []

    # bwang: need to drop table 
    def _saving_work(code, report_type, coll):
        try:
            QA_util_log_info("now saving {} report of stock {}".format(report_type, code))

            coll.insert_many(
                QA_util_to_json_from_pandas(
                    QA_fecth_local_financial_report_cn(code, report_type)
                )
            )
        except Exception as e:
            QA_util_log_info(str(e))
            err.append((code, report_type))

    futures = []
    executor = ThreadPoolExecutor(max_workers=10)
    for code in stock_list:
        futures.append(executor.submit(_saving_work, code, 'balance', coll_balance))
        futures.append(executor.submit(_saving_work, code, 'profit', coll_profit))
        futures.append(executor.submit(_saving_work, code, 'cashflow', coll_cashflow))

    count = 0
    for i in concurrent.futures.as_completed(futures):
        QA_util_log_info('The {} of Total {}'.format(count, len(stock_list)))
        count = count + 1
    if len(err) < 1:
        QA_util_log_info('SUCCESS')
    else:
        QA_util_log_info(' ERROR CODE \n ')
        QA_util_log_info(err)

    