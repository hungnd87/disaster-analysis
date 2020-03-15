import pandas as pd
import requests
import time
import datetime
from app_const import vn30

def crawl_stock_info(symbol, fromDate, toDate ):
    url = 'https://finfo-api.vndirect.com.vn/v3/stocks/intraday/history?symbols={}&sort=-time&fromDate={}&toDate={}&limit=10000000'\
        .format(symbol, fromDate, toDate)
    r = requests.get(url)
    data = r.json()
    data = data.get('data').get('hits')
    data2 = map(lambda hit: hit.get('_source'), data)
    df = pd.DataFrame(data2)
    return df

def crawl_stock_info_by_year(symbol, year):
    from_date = '{}-01-01'.format(year)
    to_date = '{}-01-01'.format(year + 1)
    df = crawl_stock_info(symbol, from_date, to_date)
    df.to_csv('data/price/{}_{}.csv'.format(symbol, str(year)))
    return df

def crawl_stock_all_year(symbol):
    for year in range(2007, 2021):
        try:
            crawl_stock_info_by_year(symbol, year)
            time.sleep(1)
        except Exception as e:
            print(e)
            print('Can not crawl {} {}'.format(symbol, year))

def crawl_stock_hlcov_all_year(symbol):
    for year in range(2007, 2021):
        try:
            crawl_stock_hlcov_by_year(symbol, year)
        except Exception as e:
            print(e)
            print('Can not crawl {} {}'.format(symbol, year))

def crawl_VN_30():
    for symbol in vn30:
        crawl_stock_hlcov_all_year(symbol)

def crawl_stock_hlcov(symbol, fromTime, toTime ):
    from_time = (fromTime-datetime.datetime(1970,1,1)).total_seconds()
    to_time = (toTime-datetime.datetime(1970,1,1)).total_seconds()
    url = 'https://dchart-api.vndirect.com.vn/dchart/history?resolution=D&symbol={}&from={}&to={}'\
        .format(symbol, round(from_time), round(to_time))
    r = requests.get(url)
    data = r.json()
    arr = []
    times = data.get('t')
    closes = data.get('c')
    opens = data.get('o')
    hights = data.get('h')
    lows = data.get('l')
    volumes = data.get('v')
    for i in range(0, len(data.get('t'))):
        arr.append({
            "time": times[i],
            "close": closes[i],
            "open": opens[i],
            "hight": hights[i],
            "low": lows[i],
            "volumes": volumes[i]
        })
    df = pd.DataFrame(arr)
    return df

def crawl_stock_hlcov_by_year(symbol, year):
    from_time = datetime.datetime(year, 1, 1)
    to_time = datetime.datetime(year + 1, 1, 1)
    df = crawl_stock_hlcov(symbol, from_time, to_time)
    df.to_csv('data/price_hlcov/{}_{}.csv'.format(symbol, str(year)))
    return df

if __name__ == '__main__':
    #df = crawl_stock_info('VND', '2010-01-01', '2019-09-09')
    #df = crawl_stock_info_by_year('VND', 2019)
    #crawl_stock_all_year('CSV')
    crawl_VN_30()
    #crawl_stock_hlcov_by_year('VNM', 2019)
