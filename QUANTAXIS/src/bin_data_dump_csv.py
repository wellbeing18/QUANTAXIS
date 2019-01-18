import pickle
from multiprocessing import Pool
from QUANTAXIS.QAFetch.QAQuery_Advance import (QA_fetch_stock_pure_tech_indicator_adv, QA_fetch_stock_tech_indicator_normalized_adv)
from QUANTAXIS.QAUtil import QA_util_log_info

def dump_csv_4_abu(code, file_path):
    try:
        file_name = file_path + str(code) + '.csv'
        df_stock = QA_fetch_stock_pure_tech_indicator_adv(code)
        del df_stock['date']
        df_stock.to_csv(file_name)
        print('successfully created {}'.format(file_name))
    except Exception as e:
        print('failed to create {} due to {}'.format(file_name, str(e)))

def dump_full_indicator_csv_4_abu(code, file_path):
    try:
        file_name = file_path + str(code) + '.csv'
        df_stock = QA_fetch_stock_tech_indicator_normalized_adv(code)
        #del df_stock['date']
        df_stock.dropna(how='all')
        df_stock.to_csv(file_name)
        print('successfully created {}'.format(file_name))
    except Exception as e:
        print('failed to create {} due to {}'.format(file_name, str(e)))

if __name__ == '__main__':
    #file_path = 'D:/abu/data/new_csv/'
    file_path = 'D:/abu/data/new_full_indicator_csv/'
    stock_list = pickle.load(open("D:/abu/data/download/tdx/stock_list.pkl", 'rb'))
    #stock_list = ['002460', '000001']

    pool = Pool(7) # 8 core

    for i, stock_code in enumerate(stock_list):
        QA_util_log_info('The %s of Total %s' % (i+1, len(stock_list)))
        QA_util_log_info('Processing Progress %s ' % str(float((i+1) / len(stock_list) * 100))[0:4] + '%')

        #_saving_work(stock_list[i], coll_fin_indicator, coll_tech_indicator)
        #pool.apply_async(dump_csv_4_abu, args=(stock_code, file_path))
        pool.apply_async(dump_full_indicator_csv_4_abu, args=(stock_code, file_path))

    pool.close()
    pool.join()