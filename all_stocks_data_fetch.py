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

def today_data(from_date,todate,conn):
    nfv = 945
    kite_conn = get_token(todate,conn)
    from_date = str(from_date)+' '+'15:29:00'
    to_date = str(todate)+' '+'15:30:00'
    unique_tokens = [1510401,1270529,340481,341249,492033,779521,260105]
#     print('unique_tokens')
#     print(unique_tokens)
    unique_stocks = ['axis','icici','hdfc','hdfcbank','kotak','sbin','banknifty']

#unique_dates = ['2020-12-14','2020-12-15']
    dummy_data_df = pd.DataFrame()
    for i in range(len(unique_tokens)):
#         print('Coming to kite_conn')
        DF = kite_conn.historical_data(instrument_token=unique_tokens[i], interval='minute', from_date= from_date , to_date= todate)
        DF = pd.DataFrame(DF)  
    
#         print(DF)
        DF['date_day'] = todate
        DF['stock'] = unique_stocks[i]
        DF['MA'] = (DF['high']+DF['low'])/2
        DF['num'] = DF['date'].astype(str).str[11:16].replace(':', '', regex=True).astype(int)
        nf = DF[DF['num']==nfv]['MA'].iloc[0]
        DF['perecentage'] = ((DF['MA']-nf)/DF['MA'])*100
        DF['MA'] = DF['perecentage']
        TBL = DF
        TBL['index'] = TBL.index
        TBL = TBL[['MA','date_day','stock','index']]
        TBL = TBL.rename(columns = {'MA':'ma','date_day':'today_date','date_day':'today_date'})
        queryString = 'SELECT * FROM today_data_graph_data WHERE today_data_graph_data.today_date = '+ "'"+todate+"'" + 'and ref_stock = '+ "'"+unique_stocks[i]+"'"
        df = pd.read_sql(queryString, con=conn)
        print('coming to df')
        print(df)
        if len(df) ==0:
            print('length of df is null')
            TBL = TBL
        else:
            maxid = df.index.max()
            print('length of df is max')
            TBL = TBL[TBL['index']>maxid]
        dummy_data_df = dummy_data_df.append(TBL)

    #dummy_data_df.to_sql('master_bank_today_data', con=conn, if_exists='append', index=False)
    return dummy_data_df

def get_from_date(con,today_date):
    access_roken = pd.read_sql('select * from public.access_token', con, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)
    maximum = access_roken['id'].max()
    access_roken = access_roken[(access_roken['to_date'] == today_date)&(access_roken['id'] == maximum)]
    from_date = access_roken['from_date'].iloc[0]
    today_date = access_roken['to_date'].iloc[0]
    token = access_roken['token'].iloc[0]
    return from_date,today_date,token

if __name__ == "__main__":
    conn = create_engine()
    today_date = datetime.today().strftime('%Y-%m-%d')
    from_date,today_date,token = get_from_date(conn,today_date)
    get_token(today_date,conn)
    tbl = today_data(from_date,today_date,conn)
    tbl.columns = ['value','today_date','ref_stock','index']
    tbl.to_sql('today_data_graph_data', con=conn, if_exists='append', index=False)