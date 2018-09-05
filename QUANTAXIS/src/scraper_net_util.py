# coding: utf-8

import urllib, urllib2
import json
import time
import random
import chardet

easymoney_table_list = ['jgdy', 'jgcg']
tonghuashun_table_list = ['dzjy']

easymoney_url_page_template_map = {
    'jgdy': 'http://data.eastmoney.com/DataCenter_V3/jgdy/xx.ashx?pagesize=50&page={}&js=var%20ngDoXCbV&param=&sortRule=-1&sortType=0&rt=48753724',
    'dzjy': 'http://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?type=DZJYXQ&token=70f12f2f4f091e459a279469fe49eca5&cmd=&st=TDATE&sr=-1&p={}&ps=50&js=var%20ATNambWH={{pages:(tp),data:(x)}}&filter=(Stype=%27EQA%27)(TDATE%3E=^2009-06-01^%20and%20TDATE%3C=^2017-08-07^)&rt=50070655',
    'gdzc': 'http://data.eastmoney.com/DataCenter_V3/gdzjc.ashx?pagesize=50&page={}&js=var%20gjDzpEvS&param=&sortRule=-1&sortType=BDKS&tabid=jzc&code=&name=&rt=50074820',
    'gdjc': 'http://data.eastmoney.com/DataCenter_V3/gdzjc.ashx?pagesize=50&page={}&js=var%20MHOHqXNR&param=&sortRule=-1&sortType=BDKS&tabid=jjc&code=&name=&rt=50074824',
    'yjyz' : 'http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=SR&sty=YJYG&fd={}&st=4&sr=-1&p={}&ps=50&js=var%20NLWSzLLR={{pages:(pc),data:[(x)]}}&stat=1&rt=50111108'
}

easymoney_referer_map = {
    'jgdy' : 'http://data.eastmoney.com/jgdy/xx.html',
    'dzjy' : 'http://data.eastmoney.com/dzjy/dzjy_mrmxa.aspx',
    'gdzc' : 'http://data.eastmoney.com/executive/gdzjc-jzc.html',
    'gdjc' : 'http://data.eastmoney.com/executive/gdzjc-jjc.html',
    'yjyz' : 'http://data.eastmoney.com/bbsj/{}/yjyz.html'
}

# user_agent 池
user_agent_list=[]
user_agent_list.append("Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 ")
user_agent_list.append("Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50")
user_agent_list.append("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1")
user_agent_list.append("Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11")
user_agent_list.append("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 ")
user_agent_list.append("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36")
user_agent_list.append("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36")


# 获取页数
def get_pages_count_easymoney(table_str, season=None):
    #url = 'http://data.eastmoney.com/DataCenter_V3/{}/xx.ashx?pagesize=50&page={}'.format(table_str, 1)
    #url += "&js=var%20ngDoXCbV&param=&sortRule=-1&sortType=0&rt=48753724"
    if season != None:
        url_template_str = easymoney_url_page_template_map[table_str]
        url = url_template_str.format(season, 1)
    else:
        url_template_str = easymoney_url_page_template_map[table_str]
        url = url_template_str.format(1)
    # wp = urllib.urlopen(url)
    request = urllib2.Request(url)
    response = urllib2.urlopen(request)
    encoded_data = response.read()
    #data = response.read().decode("gbk")
    encoding = chardet.detect(encoded_data)['encoding']
    decoded_data = encoded_data.decode(encoding, 'ignore')
    start_pos = decoded_data.index('=')
    dict_data = decoded_data[start_pos + 1:]
    if table_str != 'jgdy':
        dict_data = dict_data.replace('pages:', '"pages":')
        dict_data = dict_data.replace('data:', '"data":')
    dict = json.loads(dict_data)
    pages = dict['pages']

    return pages

'''
def get_pages_count_tonghuashun(table_str):
    #url = 'http://data.10jqka.com.cn/market/{}/field/enddate/order/desc/page/{}/ajax/1/'.format(table_str, 1)
    url_template_str = easymoney_url_page_template_map[table_str]
    url = url_template_str.format(1)
    request = urllib2.Request(url)
    response = urllib2.urlopen(request)
    encoded_data = response.read()
    #data = response.read().decode("gbk")
    encoding = chardet.detect(encoded_data)['encoding']
    decoded_data = encoded_data.decode(encoding, 'ignore')
    start_pos = decoded_data.index('=')
    json_data = decoded_data[start_pos + 1:]
    dict = json.loads(json_data)
    pages = dict['pages']
    return pages'''

# 获取链接列表
def get_url_list_easymoney(start,end, table_str, season=None):
    url_list=[]
    url_template_str = easymoney_url_page_template_map[table_str]
    while(start<=end):
        #url = '''http://data.eastmoney.com/DataCenter_V3/jgdy/xx.ashx?pagesize=50&page=%d''' %start
        #url += "&js=var%20ngDoXCbV&param=&sortRule=-1&sortType=0&rt=48753724"
        if season != None:
            url = url_template_str.format(season, start)
        else:
            url = url_template_str.format(start)
        url_list.append(url)
        start+=1
    return url_list


def compose_request_easymoney(url, table_str, season_month=None):
    request = urllib2.Request(url)
    # add Referer to the header
    #request.add_header('Referer', 'http://data.eastmoney.com/jgdy/')
    if season_month != None:
        request.add_header('Referer', easymoney_referer_map[table_str].format(season_month))
    else:
        request.add_header('Referer', easymoney_referer_map[table_str])
    # 随机从user_agent池中取user agent, add to header
    pos = random.randint(0, len(user_agent_list) - 1)
    request.add_header('User-Agent', user_agent_list[pos])
    return request

def compose_request_tonghuashun(url):
    request = urllib2.Request(url)
    # add Referer to the header
    request.add_header('Referer', 'http://data.10jqka.com.cn/market/dzjy/')
    # 随机从user_agent池中取user agent, add to header
    pos = random.randint(0, len(user_agent_list) - 1)
    request.add_header('User-Agent', user_agent_list[pos])
    return request

# 获取当前的时间戳
def get_timstamp():
    timestamp =int(int(time.time())/30)
    return str(timestamp)

'''
def get_url_list(table_str):
    if table_str in easymonry_table_list:
'''