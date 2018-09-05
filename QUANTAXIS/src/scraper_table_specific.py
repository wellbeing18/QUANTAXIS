# coding: utf-8

import urllib, urllib2
import json
import time
import chardet
import datetime
import random
import re

from sqlalchemy.orm import sessionmaker
from scraper_db_util import *
from scraper_net_util import *

def urlopen_wrapper_avoid_10060(request):
      error_time = 0
      while True:
          time.sleep(1)
          try:
              return urllib2.urlopen(request)
          except:
              error_time += 1
              if error_time == 100:
                  print "network is bad"
                  time.sleep(60)
              if error_time == 101:
                  print "network is broken"
                  break
              continue


def append_jgdy(session):
    table_str = 'jgdy'
    with open('./jgdy_page_count.txt', 'r+') as file:
        # read page number from file to use as the start
        file.seek(0)
        page_num = file.read()

        pages = get_pages_count_easymoney(table_str)
        url_list =get_url_list_easymoney(int(page_num),pages, table_str)

        count = int(page_num)

        for url in url_list:
            # compose url request
            request = compose_request_easymoney(url, table_str)
            # send request
            #response = urllib2.urlopen(request)
            response = urlopen_wrapper_avoid_10060(request)
            data = response.read()
             # 自动判断编码方式并进行解码
            encoding = chardet.detect(data)['encoding']
            # 忽略不能解码的字段
            data = data.decode(encoding,'ignore')
            start_pos = data.index('=')
            json_data = data[start_pos + 1:]
            dict = json.loads(json_data)
            list_data = dict['data']
            count+=1
            # write page number(to open next time) to file
            file.seek(0)
            file.write(str(count))
            file.truncate()
            for item in list_data:

                StartDate = item['StartDate'].encode("utf8")
                if (StartDate == ""):
                    StartDate = None
                else:
                    StartDate = datetime.datetime.strptime(StartDate, "%Y-%m-%d").date()
                SName = item['SName'].encode("utf8")
                if (SName == ""):
                    SName = None
                EndDate = item["EndDate"].encode("utf8")
                if (EndDate == ""):
                    EndDate = None
                else:
                    EndDate = datetime.datetime.strptime(EndDate, "%Y-%m-%d").date()
                Description = item['Description'].encode("utf8")
                if (Description == ""):
                    Description = None
                CompanyName = item['CompanyName'].encode("utf8")
                if (CompanyName == ""):
                    CompanyName = None
                OrgName = item['OrgName'].encode("utf8")
                if (OrgName == ""):
                    OrgName = None
                CompanyCode = item['CompanyCode'].encode("utf8")
                if (CompanyCode == ""):
                    CompanyCode = None
                Licostaff = item['Licostaff'].encode("utf8")
                if (Licostaff == ""):
                    Licostaff = None

                NoticeDate = item['NoticeDate'].encode("utf8")
                if (NoticeDate == ""):
                    NoticeDate = None
                else:
                    NoticeDate = datetime.datetime.strptime(NoticeDate, "%Y-%m-%d").date()
                Place = item['Place'].encode("utf8")
                if (Place == ""):
                    Place = None
                SCode = item["SCode"].encode("utf8")
                if (SCode == ""):
                    SCode = None
                OrgCode = item['OrgCode'].encode("utf8")
                if (OrgCode == ""):
                    OrgCode = None
                Personnel = item['Personnel'].encode('utf8')
                if (Personnel == ""):
                    Personnel = None

                OrgtypeName = item['OrgtypeName'].encode("utf8")
                if (OrgtypeName == ""):
                    OrgtypeName = None
                Orgtype = item['Orgtype'].encode("utf8")
                if (Orgtype == ""):
                    Orgtype = None
                Maincontent = item['Maincontent'].encode("utf8")
                if (Maincontent == ""):
                    Maincontent = None

                sql_str = 'replace '
                session.add(one)
                session.commit()
            print 'percent:' ,count*1.0/pages,"complete!,now ",count

def scrap_jgdy(session):
    table_str = 'jgdy'
    with open('./jgdy_page_count.txt', 'r+') as file:
        # read page number from file to use as the start
        file.seek(0)
        page_num = file.read()

        pages = get_pages_count_easymoney(table_str)
        url_list =get_url_list_easymoney(int(page_num),pages, table_str)

        count = int(page_num)

        for url in url_list:
            # compose url request
            request = compose_request_easymoney(url, table_str)
            # send request
            #response = urllib2.urlopen(request)
            response = urlopen_wrapper_avoid_10060(request)
            data = response.read()
             # 自动判断编码方式并进行解码
            encoding = chardet.detect(data)['encoding']
            # 忽略不能解码的字段
            data = data.decode(encoding,'ignore')
            start_pos = data.index('=')
            json_data = data[start_pos + 1:]
            dict = json.loads(json_data)
            list_data = dict['data']
            count+=1
            # write page number(to open next time) to file
            file.seek(0)
            file.write(str(count))
            file.truncate()
            for item in list_data:
                one = jgdy()

                StartDate = item['StartDate'].encode("utf8")
                if (StartDate == ""):
                    StartDate = None
                else:
                    StartDate = datetime.datetime.strptime(StartDate, "%Y-%m-%d").date()
                SName = item['SName'].encode("utf8")
                if (SName == ""):
                    SName = None
                EndDate = item["EndDate"].encode("utf8")
                if (EndDate == ""):
                    EndDate = None
                else:
                    EndDate = datetime.datetime.strptime(EndDate, "%Y-%m-%d").date()
                Description = item['Description'].encode("utf8")
                if (Description == ""):
                    Description = None
                CompanyName = item['CompanyName'].encode("utf8")
                if (CompanyName == ""):
                    CompanyName = None
                OrgName = item['OrgName'].encode("utf8")
                if (OrgName == ""):
                    OrgName = None
                CompanyCode = item['CompanyCode'].encode("utf8")
                if (CompanyCode == ""):
                    CompanyCode = None
                Licostaff = item['Licostaff'].encode("utf8")
                if (Licostaff == ""):
                    Licostaff = None

                NoticeDate = item['NoticeDate'].encode("utf8")
                if (NoticeDate == ""):
                    NoticeDate = None
                else:
                    NoticeDate = datetime.datetime.strptime(NoticeDate, "%Y-%m-%d").date()
                Place = item['Place'].encode("utf8")
                if (Place == ""):
                    Place = None
                SCode = item["SCode"].encode("utf8")
                if (SCode == ""):
                    SCode = None
                OrgCode = item['OrgCode'].encode("utf8")
                if (OrgCode == ""):
                    OrgCode = None
                Personnel = item['Personnel'].encode('utf8')
                if (Personnel == ""):
                    Personnel = None

                OrgtypeName = item['OrgtypeName'].encode("utf8")
                if (OrgtypeName == ""):
                    OrgtypeName = None
                Orgtype = item['Orgtype'].encode("utf8")
                if (Orgtype == ""):
                    Orgtype = None
                Maincontent = item['Maincontent'].encode("utf8")
                if (Maincontent == ""):
                    Maincontent = None

                one.StartDate = StartDate
                one.SName = SName
                one.EndDate = EndDate
                one.Description = Description
                one.CompanyName = CompanyName
                one.OrgName = OrgName
                one.CompanyCode = CompanyCode
                one.Licostaff = Licostaff
                # one.OrgSum=OrgSum
                # one.ChangePercent=ChangePercent
                one.NoticeDate = NoticeDate
                one.Place = Place
                one.SCode = SCode
                one.OrgCode = OrgCode
                one.Personnel = Personnel
                # one.Close=Close
                one.OrgtypeName = OrgtypeName
                one.Orgtype = Orgtype
                one.Maincontent = Maincontent

                session.add(one)
                session.commit()
            print 'percent:' ,count*1.0/pages,"complete!,now ",count
            # delay 1s
            #time.sleep(1)

def append_yjyz(session):
    # change!
    table_str = 'yjyz'

    # read page number from file to use as the start
    #file.seek(0)
    #page_num = file.read()

    working_season = '2017-09-30'

    last_date = ''

    pages = get_pages_count_easymoney(table_str, working_season)

    url_list =get_url_list_easymoney(1,pages, table_str, working_season)

    #count = int(page_num)

    count = 0
    for url in url_list:
        # compose url request
        request = compose_request_easymoney(url, table_str)
        # send request
        #response = urllib2.urlopen(request)
        response = urlopen_wrapper_avoid_10060(request)
        data = response.read()
         # 自动判断编码方式并进行解码
        encoding = chardet.detect(data)['encoding']
        # 忽略不能解码的字段
        decoded_data = data.decode(encoding,'ignore')
        start_pos = decoded_data.index('=')
        dict_data = decoded_data[start_pos + 1:]
        dict_data = dict_data.replace('pages:', '"pages":')
        dict_data = dict_data.replace('data:', '"data":')
        try:
            dict = json.loads(dict_data)
        except Exception as e:
            print(str(e))
            print(url)
            continue
        list_data = dict['data']
        count+=1
        # write page number(to open next time) to file
        #file.seek(0)
        #file.write(str(count))
        #file.truncate()
        for item in list_data:
            # change!
            one = yjyz()

            item_list = item.split(',')

            idx = 0
            SECUCODE = item_list[idx].encode("utf8")
            if (SECUCODE == ""):
                SECUCODE = None

            idx += 1
            SNAME = item_list[idx].encode("utf8")
            if (SNAME == ""):
                SNAME = None

            # skip 2
            idx += 3
            SHAREHOLDERNAME = item_list[idx].encode("utf8")
            if (SHAREHOLDERNAME == "-"):
                SHAREHOLDERNAME = None

            idx += 1
            ZENGJIAN = item_list[idx].encode("utf8")
            if (ZENGJIAN == ""):
                ZENGJIAN = None

            idx += 1
            if (item_list[idx] == "-"):
                CHANGEAMOUNT = None
            else:
                CHANGEAMOUNT = float(item_list[idx])

            idx += 1
            # unit is percentage (%)
            if (item_list[idx] == "-"):
                CHANGERATIO_OUT = None
            else:
                CHANGERATIO_OUT = float(item_list[idx])

            idx += 2
            if (item_list[idx] == "-"):
                HOLDAMOUNT_TOT = None
            else:
                HOLDAMOUNT_TOT = float(item_list[idx])

            idx += 1
            # unit is percentage (%)
            if (item_list[idx] == "-"):
                HOLDRATIO_TOT = None
            else:
                HOLDRATIO_TOT = float(item_list[idx])

            idx += 1
            if (item_list[idx] == "-"):
                HOLDAMOUNT_OUT = None
            else:
                HOLDAMOUNT_OUT = float(item_list[idx])

            idx += 1
            # unit is percentage (%)
            if (item_list[idx] == "-"):
                HOLDRATIO_OUT = None
            else:
                HOLDRATIO_OUT = float(item_list[idx])

            idx += 1
            CHGDATE_START = item_list[idx].encode("utf8")
            if(CHGDATE_START =="-"):
                CHGDATE_START = None

            idx += 1
            CHGDATE_END = item_list[idx].encode("utf8")
            if (CHGDATE_END == "-"):
                CHGDATE_END = None

            idx += 1
            CHGDATE_DISCLOSE = item_list[idx].encode("utf8")
            if (CHGDATE_DISCLOSE == "-"):
                CHGDATE_DISCLOSE = None

            one.SECUCODE = SECUCODE
            one.SNAME = SNAME
            one.SHAREHOLDERNAME = SHAREHOLDERNAME
            one.ZENGJIAN = ZENGJIAN
            one.CHANGEAMOUNT = CHANGEAMOUNT
            one.CHANGERATIO_OUT = CHANGERATIO_OUT
            one.HOLDAMOUNT_TOT = HOLDAMOUNT_TOT
            one.HOLDRATIO_TOT = HOLDRATIO_TOT
            one.HOLDAMOUNT_OUT = HOLDAMOUNT_OUT
            one.HOLDRATIO_OUT = HOLDRATIO_OUT
            one.CHGDATE_START = CHGDATE_START
            one.CHGDATE_END = CHGDATE_END
            one.CHGDATE_DISCLOSE = CHGDATE_DISCLOSE

            session.add(one)
            session.commit()
        print 'working season: {}, percent:'.format(working_season), count*1.0/pages,"complete!,now ",count

def scrap_yjyz(session):
    # change!
    table_str = 'yjyz'
    print 'scraping {} ......'.format(table_str)
    year_list = range(2005, 2018)
    season_list = ['-03-31', '-06-30', '-09-30', '-12-31']
    season_month_list = ['03', '06', '09', '12']
    for year in year_list:
        for season in season_list:
            # read page number from file to use as the start
            #file.seek(0)
            #page_num = file.read()

            if year == 2017 and season == '-12-31':
                    continue
            working_season = str(year) + season
            index = season_list.index(season)
            working_season_month = str(year) + season_month_list[index]

            pages = get_pages_count_easymoney(table_str, working_season)

            url_list =get_url_list_easymoney(1,pages, table_str, working_season)

            #count = int(page_num)

            count = 0
            for url in url_list:
                # compose url request
                request = compose_request_easymoney(url, table_str, working_season_month)
                # send request
                #response = urllib2.urlopen(request)
                response = urlopen_wrapper_avoid_10060(request)
                data = response.read()
                 # 自动判断编码方式并进行解码
                encoding = chardet.detect(data)['encoding']
                # 忽略不能解码的字段
                decoded_data = data.decode(encoding,'ignore')
                start_pos = decoded_data.index('=')
                dict_data = decoded_data[start_pos + 1:]
                dict_data = dict_data.replace('pages:', '"pages":')
                dict_data = dict_data.replace('data:', '"data":')
                try:
                    dict = json.loads(dict_data)
                except Exception as e:
                    print(str(e))
                    print(url)
                    continue
                list_data = dict['data']
                count+=1
                # write page number(to open next time) to file
                #file.seek(0)
                #file.write(str(count))
                #file.truncate()
                for item in list_data:
                    # change!
                    one = yjyz()

                    item_list = item.split(',')

                    idx = 0
                    SECUCODE = item_list[idx].encode("utf8")
                    if (SECUCODE == ""):
                        SECUCODE = None

                    idx += 1
                    SNAME = item_list[idx].encode("utf8")
                    if (SNAME == ""):
                        SNAME = None

                    idx += 1
                    TEXT = item_list[idx].encode("utf8")
                    if (TEXT == "-"):
                        TEXT = None


                    idx += 1
                    PERCENT = item_list[idx]
                    if (PERCENT == "-") or (PERCENT == ""):
                        PERCENT_LOW = 0
                        PERCENT_UPP = 0
                    else:
                        pattern = re.compile('\d+\.?\d*')
                        percent_list = pattern.findall(PERCENT)
                        if len(percent_list) == 2:
                            PERCENT_LOW = float(percent_list[0])
                            PERCENT_UPP = float(percent_list[1])
                        elif len(percent_list) == 1:
                            PERCENT_LOW = float(percent_list[0])
                            PERCENT_UPP = float(percent_list[0])
                        else:
                            print "stock {} season {} percent parse has error".format(SECUCODE, working_season)
                            PERCENT_LOW = 0
                            PERCENT_UPP = 0

                    idx += 1
                    TYPE = item_list[idx].encode("utf8")
                    if (TYPE == "-"):
                        TYPE = None

                    idx += 1
                    if (item_list[idx] == "-"):
                        LAST_PROFIT = None
                    else:
                        LAST_PROFIT = float(item_list[idx])

                    idx += 2
                    PUB_DATE = item_list[idx].encode("utf8")
                    if (PUB_DATE == "-"):
                        PUB_DATE = None

                    one.SECUCODE = SECUCODE
                    one.SNAME = SNAME
                    one.TEXT = TEXT
                    one.PERCENT_LOW = PERCENT_LOW
                    one.PERCENT_UPP = PERCENT_UPP
                    one.TYPE = TYPE
                    one.LAST_PROFIT = LAST_PROFIT
                    one.PUB_DATE = PUB_DATE
                    one.SEASON = working_season

                    session.add(one)
                    session.commit()
                print 'working season: {}, percent:'.format(working_season), count*1.0/pages,"complete!,now ",count

def scrap_dzjy(session):
    table_str = 'dzjy'
    with open('./dzjy_page_count.txt', 'r+') as file:
        # read page number from file to use as the start
        file.seek(0)
        page_num = file.read()

        pages = get_pages_count_easymoney(table_str)

        url_list =get_url_list_easymoney(int(page_num),pages, table_str)

        count = int(page_num)

        for url in url_list:
            # compose url request
            request = compose_request_easymoney(url, table_str)
            # send request
            #response = urllib2.urlopen(request)
            response = urlopen_wrapper_avoid_10060(request)
            data = response.read()
             # 自动判断编码方式并进行解码
            encoding = chardet.detect(data)['encoding']
            # 忽略不能解码的字段
            decoded_data = data.decode(encoding,'ignore')
            start_pos = decoded_data.index('=')
            dict_data = decoded_data[start_pos + 1:]
            dict_data = dict_data.replace('pages:', '"pages":')
            dict_data = dict_data.replace('data:', '"data":')
            dict = json.loads(dict_data)
            list_data = dict['data']
            count+=1
            # write page number(to open next time) to file
            file.seek(0)
            file.write(str(count))
            file.truncate()
            for item in list_data:
                one = dzjy()

                TDATE =item['TDATE'].encode("utf8")
                if(TDATE ==""):
                    TDATE = None
                else:
                    TDATE = datetime.datetime.strptime(TDATE,"%Y-%m-%dT%H:%M:%S").date()

                SECUCODE = item['SECUCODE'].encode("utf8")
                if (SECUCODE == ""):
                    SECUCODE = None

                SNAME=item['SNAME'].encode("utf8")
                if(SNAME ==""):
                    SNAME =None

                RCHANGE = item['RCHANGE']
                if (RCHANGE == ""):
                    RCHANGE = None

                CPRICE = item['CPRICE']
                if (CPRICE == ""):
                    CPRICE = None

                PRICE = item['PRICE']
                if (PRICE == ""):
                    PRICE = None

                Zyl = item['Zyl']
                if (Zyl == ""):
                    Zyl = None

                TVOL = item['TVOL']
                if (TVOL == ""):
                    TVOL = None

                TVAL = item['TVAL']
                if (TVAL == ""):
                    TVAL = None

                BUYERNAME=item['BUYERNAME'].encode("utf8")
                if(BUYERNAME ==""):
                    BUYERNAME= None

                SALESNAME = item['SALESNAME'].encode("utf8")
                if (SALESNAME == ""):
                    SALESNAME = None

                RCHANGE1DC = item['RCHANGE1DC']
                if (RCHANGE1DC == "-"):
                    RCHANGE1DC = None

                RCHANGE5DC = item['RCHANGE5DC']
                if (RCHANGE5DC == "-"):
                    RCHANGE5DC = None

                RCHANGE10DC = item['RCHANGE10DC']
                if (RCHANGE10DC == "-"):
                    RCHANGE10DC = None

                RCHANGE20DC = item['RCHANGE20DC']
                if (RCHANGE20DC == "-"):
                    RCHANGE20DC = None

                one.TDATE = TDATE
                one.SECUCODE = SECUCODE
                one.SNAME = SNAME
                one.RCHANGE = RCHANGE
                one.CPRICE = CPRICE
                one.PRICE = PRICE
                one.Zyl = Zyl
                one.TVOL = TVOL
                one.TVAL = TVAL
                one.BUYERNAME = BUYERNAME
                one.SALESNAME = SALESNAME
                one.RCHANGE1DC = RCHANGE1DC
                one.RCHANGE5DC = RCHANGE5DC
                one.RCHANGE10DC = RCHANGE10DC
                one.RCHANGE20DC = RCHANGE20DC

                session.add(one)
                session.commit()
            print 'percent:' ,count*1.0/pages,"complete!,now ",count
            # delay 1s
            time.sleep(1)

def scrap_gdzc(session):
    # change!
    table_str = 'gdzc'
    with open('./gdzc_page_count.txt', 'r+') as file:
        # read page number from file to use as the start
        file.seek(0)
        page_num = file.read()

        pages = get_pages_count_easymoney(table_str)

        url_list =get_url_list_easymoney(int(page_num),pages, table_str)

        count = int(page_num)

        for url in url_list:
            # compose url request
            request = compose_request_easymoney(url, table_str)
            # send request
            #response = urllib2.urlopen(request)
            response = urlopen_wrapper_avoid_10060(request)
            data = response.read()
             # 自动判断编码方式并进行解码
            encoding = chardet.detect(data)['encoding']
            # 忽略不能解码的字段
            decoded_data = data.decode(encoding,'ignore')
            start_pos = decoded_data.index('=')
            dict_data = decoded_data[start_pos + 1:]
            dict_data = dict_data.replace('pages:', '"pages":')
            dict_data = dict_data.replace('data:', '"data":')
            dict = json.loads(dict_data)
            list_data = dict['data']
            count+=1
            # write page number(to open next time) to file
            file.seek(0)
            file.write(str(count))
            file.truncate()
            for item in list_data:
                # change!
                one = gdzc()

                item_list = item.split(',')

                idx = 0
                SECUCODE = item_list[idx].encode("utf8")
                if (SECUCODE == ""):
                    SECUCODE = None

                idx += 1
                SNAME = item_list[idx].encode("utf8")
                if (SNAME == ""):
                    SNAME = None

                # skip 2
                idx += 3
                SHAREHOLDERNAME = item_list[idx].encode("utf8")
                if (SHAREHOLDERNAME == "-"):
                    SHAREHOLDERNAME = None

                idx += 1
                ZENGJIAN = item_list[idx].encode("utf8")
                if (ZENGJIAN == ""):
                    ZENGJIAN = None

                idx += 1
                if (item_list[idx] == "-"):
                    CHANGEAMOUNT = None
                else:
                    CHANGEAMOUNT = float(item_list[idx])

                idx += 1
                # unit is percentage (%)
                if (item_list[idx] == "-"):
                    CHANGERATIO_OUT = None
                else:
                    CHANGERATIO_OUT = float(item_list[idx])

                idx += 2
                if (item_list[idx] == "-"):
                    HOLDAMOUNT_TOT = None
                else:
                    HOLDAMOUNT_TOT = float(item_list[idx])

                idx += 1
                # unit is percentage (%)
                if (item_list[idx] == "-"):
                    HOLDRATIO_TOT = None
                else:
                    HOLDRATIO_TOT = float(item_list[idx])

                idx += 1
                if (item_list[idx] == "-"):
                    HOLDAMOUNT_OUT = None
                else:
                    HOLDAMOUNT_OUT = float(item_list[idx])

                idx += 1
                # unit is percentage (%)
                if (item_list[idx] == "-"):
                    HOLDRATIO_OUT = None
                else:
                    HOLDRATIO_OUT = float(item_list[idx])

                idx += 1
                CHGDATE_START = item_list[idx].encode("utf8")
                if(CHGDATE_START =="-"):
                    CHGDATE_START = None

                idx += 1
                CHGDATE_END = item_list[idx].encode("utf8")
                if (CHGDATE_END == "-"):
                    CHGDATE_END = None

                idx += 1
                CHGDATE_DISCLOSE = item_list[idx].encode("utf8")
                if (CHGDATE_DISCLOSE == "-"):
                    CHGDATE_DISCLOSE = None

                one.SECUCODE = SECUCODE
                one.SNAME = SNAME
                one.SHAREHOLDERNAME = SHAREHOLDERNAME
                one.ZENGJIAN = ZENGJIAN
                one.CHANGEAMOUNT = CHANGEAMOUNT
                one.CHANGERATIO_OUT = CHANGERATIO_OUT
                one.HOLDAMOUNT_TOT = HOLDAMOUNT_TOT
                one.HOLDRATIO_TOT = HOLDRATIO_TOT
                one.HOLDAMOUNT_OUT = HOLDAMOUNT_OUT
                one.HOLDRATIO_OUT = HOLDRATIO_OUT
                one.CHGDATE_START = CHGDATE_START
                one.CHGDATE_END = CHGDATE_END
                one.CHGDATE_DISCLOSE = CHGDATE_DISCLOSE

                session.add(one)
                session.commit()
            print 'percent:' ,count*1.0/pages,"complete!,now ",count
            # delay 1s
            time.sleep(1)

def scrap_gdjc(session):
    table_str = 'gdjc'
    with open('./gdjc_page_count.txt', 'r+') as file:
        # read page number from file to use as the start
        file.seek(0)
        page_num = file.read()

        pages = get_pages_count_easymoney(table_str)

        url_list =get_url_list_easymoney(int(page_num),pages, table_str)

        count = int(page_num)

        for url in url_list:
            # compose url request
            request = compose_request_easymoney(url, table_str)
            # send request
            #response = urllib2.urlopen(request)
            response = urlopen_wrapper_avoid_10060(request)
            data = response.read()
             # 自动判断编码方式并进行解码
            encoding = chardet.detect(data)['encoding']
            # 忽略不能解码的字段
            decoded_data = data.decode(encoding,'ignore')
            start_pos = decoded_data.index('=')
            dict_data = decoded_data[start_pos + 1:]
            dict_data = dict_data.replace('pages:', '"pages":')
            dict_data = dict_data.replace('data:', '"data":')
            dict = json.loads(dict_data)
            list_data = dict['data']
            count+=1
            # write page number(to open next time) to file
            file.seek(0)
            file.write(str(count))
            file.truncate()
            for item in list_data:
                #change
                one = gdjc()

                item_list = item.split(',')

                idx = 0
                SECUCODE = item_list[idx].encode("utf8")
                if (SECUCODE == ""):
                    SECUCODE = None

                idx += 1
                SNAME = item_list[idx].encode("utf8")
                if (SNAME == ""):
                    SNAME = None

                # skip 2
                idx += 3
                SHAREHOLDERNAME = item_list[idx].encode("utf8")
                if (SHAREHOLDERNAME == ""):
                    SHAREHOLDERNAME = None

                idx += 1
                ZENGJIAN = item_list[idx].encode("utf8")
                if (ZENGJIAN == ""):
                    ZENGJIAN = None

                idx += 1
                if (item_list[idx] == "-"):
                    CHANGEAMOUNT = None
                else:
                    CHANGEAMOUNT = float(item_list[idx])

                idx += 1
                # unit is percentage (%)
                if (item_list[idx] == "-"):
                    CHANGERATIO_OUT = None
                else:
                    CHANGERATIO_OUT = float(item_list[idx])

                idx += 2
                if (item_list[idx] == "-"):
                    HOLDAMOUNT_TOT = None
                else:
                    HOLDAMOUNT_TOT = float(item_list[idx])

                idx += 1
                # unit is percentage (%)
                if (item_list[idx] == "-"):
                    HOLDRATIO_TOT = None
                else:
                    HOLDRATIO_TOT = float(item_list[idx])

                idx += 1
                if (item_list[idx] == "-"):
                    HOLDAMOUNT_OUT = None
                else:
                    HOLDAMOUNT_OUT = float(item_list[idx])

                idx += 1
                # unit is percentage (%)
                if (item_list[idx] == "-"):
                    HOLDRATIO_OUT = None
                else:
                    HOLDRATIO_OUT = float(item_list[idx])

                idx += 1
                CHGDATE_START = item_list[idx].encode("utf8")
                if(CHGDATE_START =="-"):
                    CHGDATE_START = None

                idx += 1
                CHGDATE_END = item_list[idx].encode("utf8")
                if (CHGDATE_END == "-"):
                    CHGDATE_END = None

                idx += 1
                CHGDATE_DISCLOSE = item_list[idx].encode("utf8")
                if (CHGDATE_DISCLOSE == "-"):
                    CHGDATE_DISCLOSE = None

                one.SECUCODE = SECUCODE
                one.SNAME = SNAME
                one.SHAREHOLDERNAME = SHAREHOLDERNAME
                one.ZENGJIAN = ZENGJIAN
                one.CHANGEAMOUNT = CHANGEAMOUNT
                one.CHANGERATIO_OUT = CHANGERATIO_OUT
                one.HOLDAMOUNT_TOT = HOLDAMOUNT_TOT
                one.HOLDRATIO_TOT = HOLDRATIO_TOT
                one.HOLDAMOUNT_OUT = HOLDAMOUNT_OUT
                one.HOLDRATIO_OUT = HOLDRATIO_OUT
                one.CHGDATE_START = CHGDATE_START
                one.CHGDATE_END = CHGDATE_END
                one.CHGDATE_DISCLOSE = CHGDATE_DISCLOSE

                session.add(one)
                session.commit()
            print 'percent:' ,count*1.0/pages,"complete!,now ",count
            # delay 1s
            time.sleep(1)