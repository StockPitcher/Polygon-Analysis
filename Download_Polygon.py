# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 20:03:58 2024

@author: czhon
"""

# pip install -U polygon-api-client
from urllib.request import urlopen
import certifi
import json
import pandas as pd
import datetime as dt
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os
import numpy as np
from polygon import RESTClient
from polygon.rest.models import (
    Agg,
)
import io
import os
import csv

####################
# define functions #
####################
def read_data(client,ticker,start_date = '2020-01-01', end_date = dt.datetime.today().strftime('%Y-%m-%d'), multi = 5,  span="minute"):
    aggs = []
    for a in client.list_aggs(
                ticker=ticker, multiplier=multi, timespan=span, from_=start_date, to=end_date, limit=50000):
        aggs.append(a)

    headers = [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "transactions",
        "otc",
    ]

    data = []

    # writing data
    for agg in aggs:
        # verify this is an agg
        if isinstance(agg, Agg):
            # verify this is an int
            if isinstance(agg.timestamp, int):
                data.append(
                    {
                        "timestamp": dt.datetime.fromtimestamp(agg.timestamp / 1000),
                        "open": agg.open,
                        "high": agg.high,
                        "low": agg.low,
                        "close": agg.close,
                        "volume": agg.volume,
                        "vwap": agg.vwap,
                        "transactions": agg.transactions,
                        "otc": agg.otc,
                    }
                )
    if len(data) > 0:
        return pd.DataFrame(data).sort_values(['timestamp'], ascending = True)
    else:
        return pd.DataFrame(columns = headers)




def update_fivem_file(rest_client, ticker, ticker_active,path, start_date = dt.date(2000,1,1), end_date = dt.date.today(),multiplier = 5, timespan = 'minute'):
    try:
        
        if dt.datetime.now().hour <= 15:
            if end_date == dt.date.today():
                end_date = end_date - dt.timedelta(days = 1)
                
        if os.path.isfile(path + '/' + ticker + '.csv'):
           
            if ticker_active:
                existing_file = pd.read_csv(path + '/' + ticker + '.csv')
                max_date = dt.datetime.strptime(existing_file.timestamp.max()[0:10], '%Y-%m-%d').date() + dt.timedelta(days = 1)
                
                new_file = read_data(client,ticker,max_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'),multiplier,timespan)
                
                if len(new_file) > 0:
                    existing_file = pd.concat([existing_file,new_file], axis = 0).reset_index(drop = True)
                    existing_file.to_csv(path + '/' + ticker + '.csv', index = False)
                    return (pd.DataFrame([[ticker , True ,'Existing Series, data Download is successful']],columns = ['Ticker','Success','Comment']))
                else:
                    return (pd.DataFrame([[ticker , True ,'Existing Series, no more new data']],columns = ['Ticker','Success','Comment']))
            else:
                return (pd.DataFrame([[ticker , True ,'Existing Series, security inactive']],columns = ['Ticker','Success','Comment']))
       
        else:
            new_file = read_data(client,ticker,start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'),multiplier,timespan)
            if len(new_file) > 0:
                new_file.to_csv(path + '/' + ticker + '.csv', index = False)
                return (pd.DataFrame([[ticker , True ,'New Series, data Download is successful']],columns = ['Ticker','Success','Comment']))
            else:
                return (pd.DataFrame([[ticker , True ,'New Series, no data in DB']],columns = ['Ticker','Success','Comment']))
    except:
        return (pd.DataFrame([[ticker , False ,'Error in attempt']],columns = ['Ticker','Success','Comment']))


# rest_client = client
# ticker = 'AAPL'
# active = True
# path = 'D:/Quant Research/Data_Polygon/Data_Daily'
# start_date = dt.date(2000,1,1)
# end_date = dt.date(2024,9,24)
# end_date = dt.datetime.today()
# multiplier = 1
# timespan = 'day'


#update_fivem_file(rest_client, ticker, active,path, start_date , end_date,multiplier, timespan)

######################
# Start Loading Data #
######################

# establish connection
api_key = 'SVg4dRNlqYpPie4MOIuzgV0H3c5iLLtK'
client = RESTClient(api_key=api_key)

# Folder Path
data_base_path = 'D:/Quant Research/Data_Polygon/'
daily_path = 'Data_Daily'
fivem_path = 'Data_5m'
ETF_daily_path = 'ETF_Daily'
ETF_5m_path = 'ETF_5m'
audit_path = 'Audit'


# define ticker list
#ticker_list = ['AAPL','MSFT','META','INTC','T','NIO','TSLA']
ticker_list = [pd.DataFrame(client.list_tickers(market="stocks", active = True, limit=1000)),
               pd.DataFrame(client.list_tickers(market="stocks", active = False, limit=1000))]
ticker_list = pd.concat(ticker_list, axis = 0)

ticker_list = ticker_list.loc[((ticker_list.type == 'CS') | (ticker_list.type == 'ADRC') |  ((ticker_list.type.isnull())))].reset_index(drop = True)

####################################
# Pull data for single name stocks #
####################################

# Pull Intraday file
Intra_result = []
Intra_total = len(ticker_list)
Intra_complete = 0
with ThreadPoolExecutor(max_workers=31) as executor:
    futures = [executor.submit(update_fivem_file, client, i , j, data_base_path + fivem_path , dt.date(2000,1,1) ,  dt.datetime.today()) for i,j in zip(ticker_list.ticker,ticker_list.active)]
    for future in as_completed(futures):
        Intra_result.append(future.result())
        Intra_complete += 1
        print(f'Daily data download progress {Intra_complete}/{Intra_total}')
        
Intra_result = pd.concat(Intra_result,axis = 0).reset_index(drop = True)
Intra_result.to_csv(data_base_path + audit_path + '/audit_5m_' + dt.datetime.today().strftime('%Y-%m-%d') + '.csv', index = False)


# Pull Intraday file
daily_result = []
daily_total = len(ticker_list)
daily_complete = 0
with ThreadPoolExecutor(max_workers=31) as executor:
    futures = [executor.submit(update_fivem_file, client, i , j, data_base_path + daily_path , dt.date(2000,1,1) ,  dt.datetime.today(),1,'day') for i,j in zip(ticker_list.ticker,ticker_list.active)]
    for future in as_completed(futures):
        daily_result.append(future.result())
        daily_complete += 1
        print(f'Daily data download progress {daily_complete}/{daily_total}')
        
daily_result = pd.concat(daily_result,axis = 0).reset_index(drop = True)
daily_result.to_csv(data_base_path + audit_path + '/audit_daily_' + dt.datetime.today().strftime('%Y-%m-%d') + '.csv', index = False)


############################
# Pull data for ETF ticker #
############################

ETF_Ticker = ['TQQQ','QQQ','SPY','SPXL','IWM']
# Pull Intraday file
ETF_Intra_result = []
ETF_Intra_total = len(ETF_Ticker)
ETF_Intra_complete = 0
with ThreadPoolExecutor(max_workers=31) as executor:
    futures = [executor.submit(update_fivem_file, client, i , True, data_base_path + ETF_5m_path , dt.date(2000,1,1) ,  dt.datetime.today()) for i in ETF_Ticker]
    for future in as_completed(futures):
        ETF_Intra_result.append(future.result())
        ETF_Intra_complete += 1
        print(f'Five min data download progress {ETF_Intra_complete}/{ETF_Intra_total}')
        
ETF_Intra_result = pd.concat(ETF_Intra_result,axis = 0).reset_index(drop = True)
ETF_Intra_result.to_csv(data_base_path + audit_path + '/ETF_audit_5m_' + dt.datetime.today().strftime('%Y-%m-%d') + '.csv', index = False)


# Pull Intraday file

ETF_daily_result = []
ETF_daily_total = len(ticker_list)
ETF_daily_complete = 0
with ThreadPoolExecutor(max_workers=31) as executor:
    futures = [executor.submit(update_fivem_file, client, i , True, data_base_path + ETF_daily_path , dt.date(2000,1,1) ,  dt.datetime.today(),1,'day') for i in ETF_Ticker]
    for future in as_completed(futures):
        ETF_daily_result.append(future.result())
        daily_complete += 1
        print(f'Daily data download progress {daily_complete}/{daily_total}')
        
daily_result = pd.concat(daily_result,axis = 0).reset_index(drop = True)
daily_result.to_csv(data_base_path + audit_path + '/ETF_audit_daily_' + dt.datetime.today().strftime('%Y-%m-%d') + '.csv', index = False)



