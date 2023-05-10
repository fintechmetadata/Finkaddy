# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import pandas as pd
import numpy as np
from fastdtw import fastdtw
from scipy.spatial.distance import euclidean
from scipy import stats
from fastdtw import fastdtw
from scipy.spatial.distance import euclidean
from scipy import stats
from kiteconnect import KiteConnect
import sqlalchemy
#from pandasql import sqldf, load_meat, load_births
import time
from datetime import date
from datetime import datetime

def create_engine():
    engine = sqlalchemy.create_engine('postgresql://postgres:RAghava6*@testdb.cnuma5tc0a6e.us-east-2.rds.amazonaws.com:5432/postgres')
    con = engine.connect()
    return con

def get_token(to_date,conn):
    access_token = pd.read_sql('select * from public.access_token', conn, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)
    token = access_token[access_token['to_date'] == to_date]['token'].iloc[0]
    api_key= 'qvpdkf2j2f644e4n'
    api_secret='751xv1m423hrppxur0sekwcy6bvyhsua'
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(token)
    return kite

def get_from_date(con,today_date):
    access_roken = pd.read_sql('select * from public.access_token', con, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)
    maximum = access_roken['id'].max()
    access_roken = access_roken[(access_roken['to_date'] == today_date)&(access_roken['id'] == maximum)]
    from_date = access_roken['from_date'].iloc[0]
    today_date = access_roken['to_date'].iloc[0]
    token = access_roken['token'].iloc[0]
    return from_date,today_date,token

def create_minmax(from_date_input,to_date_input,con,kite):
    res_main_x = pd.read_sql('select * from public.axis_res_main', con, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)
    res_main_x = res_main_x.rename(columns = {'Date':'date_day'})

    res_main_x = pd.DataFrame(res_main_x)
    res_main_x = pd.DataFrame(res_main_x[(res_main_x['stock'] == 'axis')&(res_main_x['today_date'] == to_date_input)]).reset_index(drop = True)
    bnk = pd.read_sql('select * from public.allstocks_min_43_min_data', con, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)
    bnk = pd.merge(bnk,res_main_x[['date_day','stock']])
    #print(bnk)
    #print(x)


    from_date = str(from_date_input)+' '+'15:29:00'
    to_date = str(to_date_input)+' '+'09:43:00'
    DF = kite.historical_data(instrument_token=1510401, interval='minute', from_date= from_date , to_date= to_date)
    DF = pd.DataFrame(DF)
    DF['date_day'] = to_date_input
    DF['stock'] = 'hcl'
    DF['MA'] = (DF['high']+DF['low'])/2
    #DF = DF[:375]
    TBL = DF
    unique_dates = TBL['date_day'].unique()
    #unique_dates = ['2020-12-14','2020-12-15']
    dummy_data_df = pd.DataFrame()
    for i in range(len(unique_dates)):    
        print(unique_dates[i])
        dummy_data = pd.DataFrame(TBL[TBL['date_day'] ==unique_dates[i]])
        dummy_data_MA = pd.DataFrame(stats.zscore(dummy_data['MA']))
        dummy_data_MA = dummy_data_MA.reset_index(drop = True)
        #print(dummy_data_MA.head())
       # print(dummy_data_MA.shape)
        dummy_data_MA_T = dummy_data_MA.T
        dummy_data_MA_T['date_day'] = unique_dates[i]
        #dummy_data_MA_T = 
        dummy_data_df = dummy_data_df.append(dummy_data_MA_T)
    dummy_data_df = dummy_data_df.reset_index(drop = True)
    #dummy_data_df = dummy_data_df.drop(['date_day'],axis = 1)
    X_test = pd.DataFrame(dummy_data_df)
    X_test = X_test.rename(columns = {421:'date_day'})
    dummy_data_df = X_test
    pd.DataFrame(X_test)
    reference = dummy_data_df.drop(['date_day'], axis = 1).reset_index(drop = True)
    ref3 = list(reference.iloc[0])
    ##########################################
    #nifty_bank_ninethirty_data
    bnk = pd.DataFrame(bnk)
    bnk = bnk.fillna(0)
    bnk = bnk[bnk['stock']=='axis'].reset_index(drop = True)

    #print(bnk.head())
    date_rangelen = bnk['0'].unique()
    reference = ref3
    #ref3 = list(reference.iloc[0])
    tail = bnk
    dist = []
    date_df = pd.DataFrame()
    for i in range(len(date_rangelen)) :
        #print(dummy_data_df['date_day'])
        #print(pd.DataFrame(dummy_data_df[i]['Date_day']))
        dummy_data_df_rm = pd.DataFrame(bnk.drop(['date_day','stock'], axis = 1))
        #print('dropped')
        dummy_data_df_rm = list(dummy_data_df_rm.loc[i])
        distance, path = fastdtw(dummy_data_df_rm, reference, dist=euclidean)
        #distance = pd.DataFrame(distance)
        #print(distance)
        date_df
        
        dist.append(distance)
    res = pd.DataFrame(dist)
    #res
    res['Date'] = bnk['date_day']
    close = DF.tail(1)['close'].iloc[0]
    res = res.rename(columns = {0:'Nearest_neighbours'})
    res_main = res.sort_values(by=['Nearest_neighbours'],ascending=True).reset_index(drop = True).head(10)
    #res_main Nearest_neighbours
    #print(res_main)
    res_main = res_main[res_main['Nearest_neighbours']!=0].reset_index(drop = True).head(3)
    res_main['stock'] = 'axis'
    res_main['today_date'] = today_date   
    print('Coming to resmain')
    print(res_main)
    res_copy = res_main.copy()
    min_max_nrml = pd.read_sql('select * from public.min_max_df_allstocks', con, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)
    min_max_nrml = min_max_nrml.rename(columns = {'date_day':'Date'})
    minmaxdata_nrml = pd.merge(min_max_nrml,res_main,how = 'inner')
    DF = kite.historical_data(instrument_token=1510401, interval='5minute', from_date=  str(today_date)+' '+'09:45:00' , to_date= str(today_date)+' '+'09:45:00')
    DF = pd.DataFrame(DF)
    nfve = DF['close'].iloc[0]
    nfve
    minmaxdata_nrml['nfv'] = nfve
    minmaxdata_nrml =  minmaxdata_nrml.sort_values(by=['Nearest_neighbours'],ascending=True).reset_index(drop = True)
    
    min_max_nrml = pd.read_sql('select * from public.min_max_df_allstocks', con, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)
    min_max_nrml = min_max_nrml.rename(columns = {'date_day':'Date'})
    minmaxdata_nrml = pd.merge(min_max_nrml,res_main,how = 'inner')
    DF = kite.historical_data(instrument_token=1510401, interval='5minute', from_date=  str(today_date)+' '+'09:45:00' , to_date= str(today_date)+' '+'09:45:00')
    
    DF = pd.DataFrame(DF)
    nfve = DF['close'].iloc[0]
    minmaxdata_nrml['nfv'] = nfve
    minmaxdata_nrml =  minmaxdata_nrml.sort_values(by=['Nearest_neighbours'],ascending=True).reset_index(drop = True)
    minmaxdata_nrml['max_value'] = ((minmaxdata_nrml['max']*minmaxdata_nrml['nfv'])/100)+minmaxdata_nrml['nfv']
    minmaxdata_nrml['min_value'] = ((minmaxdata_nrml['min']*minmaxdata_nrml['nfv'])/100)+minmaxdata_nrml['nfv']
    minmaxdata_nrml = minmaxdata_nrml[['Date','max_value','min_value','stock']]
    minmaxdata_nrml['today_date'] = today_date
    minmaxdata_nrml['ref_stock'] = 'axis'
    minmaxdata_nrml = minmaxdata_nrml.rename(columns = {'Date':'date'})
    minmaxdata_nrml = minmaxdata_nrml[['date','today_date','max_value','min_value','stock','ref_stock']]
    return minmaxdata_nrml
if __name__ == "__main__":
    conn = create_engine()
    today_date = datetime.today().strftime('%Y-%m-%d')
    from_date,today_date,token = get_from_date(conn,today_date)
    kite_conn = get_token(today_date,conn)
    minmaxdata_nrml = create_minmax(from_date,today_date,conn,kite_conn)
    minmaxdata_nrml.to_sql('minmaxdata_today', con=conn, if_exists='append', index=False)   