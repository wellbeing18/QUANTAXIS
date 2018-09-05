# coding: utf-8

import pymongo
from pymongo import MongoClient
import urllib.request as urllib2
import requests
from bs4 import BeautifulSoup
import time
import random
import chardet
import pandas as pd
import tushare as ts
import re
import json
from selenium import webdriver
import numpy as np
import datetime
import pickle as cPickle

column_name = ['公报日期', '基本每股收益(元)', '净利润(元)', '净利润同比增长率', '扣非净利润(元)', '扣非净利润同比增长率',
               '营业总收入(元)', '营业总收入同比增长率', '每股净资产(元)', '净资产收益率', '净资产收益率-摊薄', '资产负债比率',
               '每股资本公积金(元)', '每股未分配利润(元)', '每股经营现金流(元)', '销售毛利率', '存货周转率', '销售净利率']
column = ['date', 'jbmgsy', 'jlr', 'jlrtbzzl', 'kfjlr', 'kfjlrtbzzl', 'yyzsr', 'yyzsrtbzzl', 'mgjzc', 'jzcsrl', 'jzcsyl-tb',
               'zcfzbl', 'mgzbgjj', 'mgwfplr', 'mgjyxjl', 'xsmll', 'chzzl', 'xsjll']

ip = '127.0.0.1'
port = 27017

client = MongoClient(ip, port)
db = client.quantaxis
coll = db.fin_data

# scrape financial data

# get stock list
data = ts.get_stock_basics()
stock_list = data[data['timeToMarket'] != 0]['name']

coll.create_index([("code",pymongo.ASCENDING)])
url_template = 'http://basic.10jqka.com.cn/{}/finance.html'

finished_list = []
error_list = []
flawed_list = []

#stock_list = ['603799', '603180'] # '300618'
for idx  in range(len(stock_list)):
    stock_id = stock_list.index[idx]
    stock_name = stock_list[idx]

    # skip existing records
    if coll.find({'code': stock_id}).count() != 0:
        finished_list.append(stock_id)
        continue

    #if stock_id in finished_list or stock_id in flawed_list:
        #continue

    url = url_template.format(stock_id)

    try:
        # compose request
        # when javascript rendered page, cannot use urllib
        # use selenium
        #driver = webdriver.Chrome()
        driver = webdriver.PhantomJS()
        driver.set_page_load_timeout(20)
        try:
            start_time = time.time()
            driver.get(url)
            print('fetch page for stock {} by time: {}'.format(stock_id, time.time() - start_time))
            html_source = driver.page_source
            driver.quit()
        except:
            print("Page load Timeout Occured for stock {}".format(stock_id))
            driver.quit()
            error_list.append(stock_id)
            continue

        soup = BeautifulSoup(html_source, "lxml")

        '''
        with open('html.txt', 'w', encoding='utf-8') as f:
            print(soup.prettify(), file=f)
        '''

        data_tbody = soup.find("div", class_="data_tbody")
        top_thead = data_tbody.find('table', class_='top_thead')
        tbody = data_tbody.find('table', class_='tbody')

        # process top head to get finance date first
        items = top_thead.findAll('div', class_='td_w')
        dates = []
        for item in items:
            dates.append(item.text.strip())

        # process table body
        rows = tbody.findAll('tr')
        finance_tbl = []
        #assert(len(rows) == 17)
        if len(rows) != 17:
            flawed_list.append(stock_id)
            continue

        for i, row in enumerate(rows):
            if i == 0:
                items = row.findAll('div', class_='td_w')
            else:
                items = row.findAll('td')
            row_list = [None if item.text.strip() == "--" else item.text.strip() for item in items ]
            finance_tbl.append(row_list)

        # total # 17

        data_tbl = [dates] + finance_tbl
        data_tbl_stack = np.column_stack(data_tbl)

        df = pd.DataFrame(data_tbl_stack, columns=column).set_index('date', drop=False)
        #df['date'] = pd.to_datetime(df['date'])
        #df.set_index('date', inplace=True)

        if 'date' in df.columns:
            df.date = df.date.apply(str)

        doc_dic ={
            'code' : stock_id,
            'name': stock_name,
            'last_release_date' : df.ix[0, 'date'],
            'last_update_date': datetime.datetime.today().strftime('%Y-%m-%d'),
            'fin_data' : json.loads(df.to_json(orient='records')) # turns df into json, then into dictionary
        }

        coll.insert(doc_dic)
    except Exception as e:
        print('some error occurs when processing stock {} : {} '.format(stock_id, str(e)))
        error_list.append(stock_id)
        continue

    finished_list.append(stock_id)


cPickle.dump(error_list, open('err_list.pkl', 'wb'))
cPickle.dump(finished_list, open('finish_list.pkl', 'wb'))





