from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_fin_indicator_adv, QA_fetch_stock_tech_indicator_adv
from QUANTAXIS.QASU.save_tushare import QA_SU_save_stock_financial

"""
stock_code = '002322'
#start_date = ''
df_tech = QA_fetch_stock_tech_indicator_adv(stock_code, keys='PE')
print(df_tech.head())
"""

QA_SU_save_stock_financial()