from pymongo import MongoClient

#from QUANTAXIS.QAFetch.QATdx import select_best_ip, QA_fetch_get_stock_day, best_ip
from QUANTAXIS.QASU.save_tdx import  QA_SU_save_index_day


ip = '127.0.0.1'
port = 27017

client = MongoClient(ip, port)
db = client.quantaxis

#best_ip = select_best_ip()

QA_SU_save_index_day()

