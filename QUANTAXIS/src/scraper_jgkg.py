# coding: utf-8
import urllib.request as urllib2
import chardet
from bs4 import BeautifulSoup
import pickle as cPickle
import re
import pandas as pd
import time
'''
# 1. stock list
path = '../data/'
file_name = 'stock_list.pkl'
stock_list = cPickle.load(open(path+file_name, 'rb'))

# 2. url list composed from stock list
url_template =  'http://data.eastmoney.com/stockcomment/{}.html'
url_list = []
for stock_id in stock_list:
    url_list.append(url_template.format(stock_id))

cPickle.dump(url_list, open(path+'url_list.pkl', 'wb'))


# 3. open url link and constructed as bs object
url_list = cPickle.load(open('../data/url_list.pkl', 'rb'))
'''


def urlopen_wrapper_avoid_10060(request):
    error_time = 0
    while True:
        time.sleep(1)
        try:
            return urllib2.urlopen(request)
        except:
            error_time += 1
            if error_time == 100:
                print("network is bad")
                time.sleep(60)
            if error_time == 101:
                print("network is broken")
                break
            continue

path = '../data/'
file_name = 'stock_list.pkl'
#stock_list = cPickle.load(open(path+file_name, 'rb'))
stock_list = ['300618']

url_template =  'http://data.eastmoney.com/stockcomment/{}.html'

pattern1 = re.compile('\d+\.\d+%')
pattern2 = re.compile('完全控盘|中度控盘|轻度控盘|不控盘')

zlkg_list = []
col_labels = ['stock_id', 'percentage', 'type']

for stock_id in stock_list:
    print('scraping stock: {}'.format(stock_id))
    url = url_template.format(stock_id)

    request = urllib2.Request(url)
    response = urlopen_wrapper_avoid_10060(request)
    encoded_data = response.read()
    # decode for GB2312
    encoding = chardet.detect(encoded_data)['encoding']
    decoded_data = encoded_data.decode(encoding, 'ignore')

    soup = BeautifulSoup(decoded_data, "lxml")

    zjkg_table = soup.find("div", class_="detail_zlkp_wrap")
    try:
        kg_txt = zjkg_table.find_next('span', id='dv_empty0').renderContents()
    except AttributeError:
        print('error in stock {}'.format(stock_id))
        kg_txt = ' '

    try:
        percent = pattern1.search(kg_txt).group()
    except AttributeError:
        percent = 0

    try:
        status_str = pattern2.search(kg_txt).group()
    except AttributeError:
        status_str = None

    zlkg_list.append((stock_id, percent, status_str))

df = pd.DataFrame.from_records(zlkg_list, columns=col_labels)
df.to_csv(path+'jgkg.csv')


# 4. search for 'div class='detail_zlkp_wrap', then '<span id="dv_empty0">注解：机构参与度为42.79%，属于完全控盘</span>'