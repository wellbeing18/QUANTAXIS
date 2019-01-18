from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_day_ts_adv

"""
stock_id = '002098'
df_002098 = QA_fetch_stock_day_ts_adv(stock_id)
"""
from QUANTAXIS.QAFetch.QATushare import QA_fetch_get_stock_day
from QUANTAXIS.QAUtil.QASetting import DATABASE
import datetime
from QUANTAXIS.QAUtil import (DATABASE, QA_util_get_next_day,
                              QA_util_get_real_date, QA_util_log_info,
                              QA_util_to_json_from_pandas, trade_date_sse)
from multiprocessing import Pool
import tushare as ts

from QUANTAXIS import QA_fetch_stock_day_ts_adv
from QUANTAXIS.QASU.save_df_2_db import QA_SU_append_stock_tech_indicator
import pandas as pd 

def main():
    client=DATABASE

    df = ts.get_stock_basics()
    coll_stock_day = client.stock_day_ts
    coll_stock_day.ensure_index('code')

    def now_time():
        return str(QA_util_get_real_date(str(datetime.date.today() - datetime.timedelta(days=1)), trade_date_sse, -1)) + \
            ' 17:00:00' if datetime.datetime.now().hour < 15 else str(QA_util_get_real_date(
                str(datetime.date.today()), trade_date_sse, -1)) + ' 15:00:00'

    def saving_work(code, coll_stock_day):
        try:
            print("entering saving_work {}".format(code))
            ref = coll_stock_day.find({'code': str(code)[:6]})
            end_date = str(now_time())[:10]

            if ref.count() > 0:
                start_date = ref[ref.count() - 1]['date']
                QA_util_log_info('UPDATE_STOCK_DAY \n Trying updating {} from {} to {}'.format(
                                    code, start_date, end_date))
                if start_date != end_date:
                    start_date = QA_util_get_next_day(start_date)
                    data_json = QA_fetch_get_stock_day(str(code), start=start_date, end=end_date, type_='json')
                    coll_stock_day.insert_many(data_json)
            else:
                start_date = '1990-01-01'
                data_json = QA_fetch_get_stock_day(str(code), start=start_date, type_='json')
                coll_stock_day.insert_many(data_json)
            print("done saving_work {}".format(code))
        except Exception as e:
            QA_util_log_info('error in saving ==== %s' % str(code))
            print("The exception is {}".format(str(e)))

    # bwang: multiprocess async
    pool = Pool(1)

    for i_ in range(len(df.index)):
        QA_util_log_info('The %s of Total %s' % (i_, len(df.index)))
        QA_util_log_info('DOWNLOAD PROGRESS %s ' % str(
            float(i_ / len(df.index) * 100))[0:4] + '%')
        #saving_work(df.index[i_], coll_stock_day)
        pool.apply_async(saving_work, args=(df.index[i_], coll_stock_day))

    pool.close()
    pool.join()

if __name__ == "__main__":
    #main()


    df_sz = QA_SU_append_stock_tech_indicator('002460', start='2018-09-01')