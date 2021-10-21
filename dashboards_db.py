# -*- coding: utf-8 -*-
"""
Created on Fri Oct  1 05:12:06 2021

@author: Trader_Mike
"""
# -*- coding: utf-8 -*-
"""
Created on Wed May  5 22:33:36 2021

@author: Mangesh
"""
import datetime as dt
from datetime import datetime
import pandas as pd
import time
import os
import schedule
import talib
import math
import numpy as np
import psycopg2
import json
from playsound import playsound
from sqlalchemy import create_engine
engine = create_engine('')
connection = psycopg2.connect(
            host="",
            database="",
            user="",
            password="")
sector_all_data=pd.read_sql('select * from bhav_copy_raw.index_file order by "Index Name","Index Date"', connection,index_col='index')
stock_all_data=data=pd.read_sql('select * from bhav_copy_raw.bhavcopy order by "SYMBOL","TIMESTAMP"', connection,index_col='index')
        
def get_historicaldata(symbol,index):
    print(symbol)
    if index:
        data=sector_all_data.loc[sector_all_data['Index Name']==symbol].sort_values(by=['Index Date'])
        data=data.rename({'Index Date':'date','Open Index Value':'open','High Index Value':'high','Low Index Value':'low','Closing Index Value':'close','Volume':'volume'}, axis=1)
        data=data.loc[:,['date','open','high','low','close','volume']]
        data=data.reset_index(drop=True)
        return data
    else:
        data=stock_all_data.loc[stock_all_data.SYMBOL==symbol].sort_values(by=['TIMESTAMP'])
        del data['SERIES']
        data=data.rename({'TIMESTAMP':'date','OPEN':'open','HIGH':'high','LOW':'low','CLOSE':'close','TOTTRDQTY':'volume'}, axis=1)
        data=data.loc[:,['date','open','high','low','close','volume']]
        data=data.reset_index(drop=True)
        return data
        
def processdata(df):
    df['ST']=supertrendfunction(df,10,2,df)
    a,b,df['MACD']=talib.MACD(df["close"],12,26,9)
    return df

def paddata(df3):
    cache=pd.DataFrame(columns=df3.columns)
    for i in range(0,len(df3)):
        if not math.isnan(df3['open_y'][i]):
            cache=df3.loc[i]
        else:
            df3['open_y'][i]=cache['open_y']
            df3['low_y'][i]=cache['low_y']
            df3['high_y'][i]=cache['high_y']
            df3['close_y'][i]=cache['close_y']
            df3['volume_y'][i]=cache['volume_y'] 
            df3['ST_y'][i]=cache['ST_y']
            df3['MACD_y'][i]=cache['MACD_y']
    return df3

def getdata(symbol,index):
    datanf=get_historicaldata(symbol,index)
    datanf=processdata(datanf)
    datanfweekly=weeklycandle(datanf)
    datanfweekly=processdata(datanfweekly)
    datanf=pd.merge(datanf,datanfweekly,on=['date'],how='outer')
    datanf=paddata(datanf)
    datanf.drop(['open_x','high_x','low_x','open_y','high_y','low_y','volume_x','Week_number_x','Year_x','Week_number_y','Year_y','volume_y'],axis=1,inplace=True)
    return datanf
def weeklycandle(df):
    df['date'] = pd.to_datetime(df['date'])
    # Getting month number
    df['Week_number'] = df['date'].dt.week
    # Getting year. month is common across years (as if you dont know :) )to we need to create unique index by using year and month
    df['Year'] = df['date'].dt.year
    df2 = df.groupby(['Year','Week_number']).agg({'date':'first','open':'first', 'high':'max', 'low':'min', 'close':'last','volume':'sum'})
    df2=df2.reset_index()
    return df2
def supertrendfunction(DF,n,m,data):
    """function to calculate Supertrend given historical candle data
        n = n day ATR - usually 7 day ATR is used
        m = multiplier - usually 2 or 3 is used"""
    df = DF.copy()
    df['ATR'] = talib.ATR(data['high'], data['low'], data['close'], n)
    df["B-U"]=((df['high']+df['low'])/2) + m*df['ATR'] 
    df["B-L"]=((df['high']+df['low'])/2) - m*df['ATR']
    df["U-B"]=df["B-U"]
    df["L-B"]=df["B-L"]
    ind = df.index
    for i in range(n,len(df)):
        if df['close'][i-1]<=df['U-B'][i-1]:
            df.loc[ind[i],'U-B']=min(df['B-U'][i],df['U-B'][i-1])
        else:
            df.loc[ind[i],'U-B']=df['B-U'][i]    
    for i in range(n,len(df)):
        if df['close'][i-1]>=df['L-B'][i-1]:
            df.loc[ind[i],'L-B']=max(df['B-L'][i],df['L-B'][i-1])
        else:
            df.loc[ind[i],'L-B']=df['B-L'][i]  
    df['Strend']=np.nan
    for test in range(n,len(df)):
        if df['close'][test-1]<=df['U-B'][test-1] and df['close'][test]>df['U-B'][test]:
            df.loc[ind[test],'Strend']=df['L-B'][test]
            break
        if df['close'][test-1]>=df['L-B'][test-1] and df['close'][test]<df['L-B'][test]:
            df.loc[ind[test],'Strend']=df['U-B'][test]
            break
    for i in range(test+1,len(df)):
        if df['Strend'][i-1]==df['U-B'][i-1] and df['close'][i]<=df['U-B'][i]:
            df.loc[ind[i],'Strend']=df['U-B'][i]
        elif  df['Strend'][i-1]==df['U-B'][i-1] and df['close'][i]>=df['U-B'][i]:
            df.loc[ind[i],'Strend']=df['L-B'][i]
        elif df['Strend'][i-1]==df['L-B'][i-1] and df['close'][i]>=df['L-B'][i]:
            df.loc[ind[i],'Strend']=df['L-B'][i]
        elif df['Strend'][i-1]==df['L-B'][i-1] and df['close'][i]<=df['L-B'][i]:
            df.loc[ind[i],'Strend']=df['U-B'][i]
    return df['Strend']

def getsector():
    sectors=pd.read_csv(r"")
    nifty=getdata("NIFTY 50",True)
    nifty.columns=["date","closenf","STnf","MACDnf","closenfh","STnfh","MACDnfh"]
    data=pd.DataFrame(columns=['date', 'closesect', 'STsect', 'MACDsect', 'closesecth', 'STsecth',
       'MACDsecth'])
    for sector in sectors['INDEX']:
        sectordata=getdata(sector,True)
        sectordata.insert(0,"sector",sector)
        sectordata.columns=['sector','date','closesect','STsect','MACDsect','closesecth','STsecth','MACDsecth']
        data=data.append(sectordata)
    return data
#sectordata=getsector()
def getstockresult(srcstock_path,dststock_path,db_name):
    stocks=pd.read_csv(srcstock_path)
    sectors=pd.read_csv(r"")
    stockslist=pd.merge(stocks,sectors,on=['INDEX_ID'],how='inner')
    stockslist.set_index("Stock_ID",inplace=True)
    result=pd.DataFrame(columns=['stock', 'date', 'close', 'ST', 'MACD','closeh', 'STh', 'MACDh',
       'NIFTY 50', 'closenf', 'STnf', 'MACDnf', 'closenfh', 'STnfh', 'MACDnfh',
       'sector', 'closesect', 'STsect', 'MACDsect', 'closesecth', 'STsecth',
       'MACDsecth', 'Sector_RS', 'Bench_RS', 'MACD_S', 'MACD_B', 'RSI',
       '52W_High', '52W_Low', 'mscore'])
    nifty=getdata("NIFTY 50",True)
    nifty.columns=["date","closenf","STnf","MACDnf","closenfh","STnfh","MACDnfh"]
    print(nifty.tail())
    sectordata=getsector()
    for stock in stockslist['Stock_name']:
        sectorindex=stockslist.loc[stockslist.Stock_name==stock].INDEX.values[0]
        stockdata=getdata(stock,False)
        stockdata.columns=['date','close','ST','MACD','closeh','STh','MACDh']
        stockdata.insert(0,"stock",stock)
        stockdata['NIFTY 50']='NIFTY 50'
        stockdata=pd.merge(stockdata,nifty,on=['date'],how='outer')
        stockdata=pd.merge(stockdata,sectordata.loc[sectordata.sector.isin([sectorindex])],on=['date'],how='outer')
        stockdata=stockdata.dropna()
        stockdata["Sector_RS"]=stockdata['close']/stockdata['closesect']
        stockdata["Bench_RS"]=stockdata['close']/stockdata['closenf']
        a,b,stockdata['MACD_S']=talib.MACD(stockdata["Sector_RS"],12,26,9)
        a,b,stockdata['MACD_B']=talib.MACD(stockdata["Bench_RS"],12,26,9)
        stockdata['RSI']=talib.RSI(stockdata["close"],14)
        stockdata['52W_High']=stockdata['close'].rolling(250).max()
        stockdata['52W_Low']=stockdata['close'].rolling(250).min()
        # stockdata['52W_High']=max(stockdata.tail(250)['close'])
        # stockdata['52W_Low']=min(stockdata.tail(250)['close'])
        stockdata['mscore']=stockdata['close'].iloc[-1]/stockdata['close'].iloc[-30]*100
        stockdata['Trend_analysis']='NA'
        stockdata['RS_analysis']='NA'
        stockdata['Strength_analysis']='NA'
        stockdata['52W high']=(stockdata["close"]-stockdata['52W_High'])/stockdata['52W_High']*100
        stockdata.loc[(stockdata['close'] > stockdata['ST']) & (stockdata['close'] > stockdata['STh']),'Trend_analysis']='Bullish'
        stockdata.loc[(stockdata['close'] < stockdata['ST']) | (stockdata['close'] < stockdata['STh']),'Trend_analysis']='Bearish'
        stockdata.loc[(stockdata['MACD_S'] > 0) & (stockdata['MACD_B'] > 0),'RS_analysis']='Bullish'
        stockdata.loc[(stockdata['MACD_S'] < 0) | (stockdata['MACD_B'] < 0),'RS_analysis']='Bearish'
        stockdata.loc[(stockdata['MACD'] > 0) & (stockdata['MACDh'] > 0),'Strength_analysis']='Bullish'
        stockdata.loc[(stockdata['MACD'] < 0) | (stockdata['MACDh'] < 0),'Strength_analysis']='Bearish'
        stockdata=stockdata[["stock","Trend_analysis","RS_analysis","Strength_analysis","52W high","mscore","date","close","ST","closeh","STh","NIFTY 50","closenf","STnf","closenfh","STnfh","sector"
                   ,"closesect","STsect","closesecth","STsecth","Sector_RS","Bench_RS","MACD_S","MACD_B","52W_High","52W_Low","MACD","MACDh","RSI"]]
    
        stockdata.to_sql(stock,engine,'db_stocks',if_exists='replace')
        result=result.append(stockdata)
    print('done')
    result=result.reset_index(drop=True)
    result=result[["stock","Trend_analysis","RS_analysis","Strength_analysis","52W high","mscore","date","close","ST","closeh","STh","NIFTY 50","closenf","STnf","closenfh","STnfh","sector"
                   ,"closesect","STsect","closesecth","STsecth","Sector_RS","Bench_RS","MACD_S","MACD_B","52W_High","52W_Low","MACD","MACDh","RSI"]]
    result.to_csv(r"")
    result = result.rename(columns={'52W high': 'one_year_high', 'NIFTY 50': 'NIFTY_50','52W_High':'one_year_high_abs','52W_Low':'one_year_low_abs'})
    result.to_sql(db_name,engine,'db_screener',if_exists='replace')
    # result['Trend_analysis']='NA'
    # result['RS_analysis']='NA'
    # result['Strength_analysis']='NA'
    # result['52W high']=(result["close"]-result['52W_High'])/result['52W_High']*100
    # for i in range(0,len(result)):
    #     if result['close'].loc[i] > result['ST'].loc[i] and result['close'].loc[i] > result['STh'].loc[i]:
    #         result['Trend_analysis'].loc[i]="Bullish"
    #     else:
    #         result['Trend_analysis'].loc[i]="Bearish"
    #     if result['MACD_S'].loc[i] > 0 and result['MACD_B'].loc[i] > 0:
    #         result['RS_analysis'].loc[i]="Bullish"
    #     else:
    #         result['RS_analysis'].loc[i]="Bearish"
    #     if result['MACD'].loc[i] > 0 and result['MACDh'].loc[i] > 0:
    #         result['Strength_analysis'].loc[i]="Bullish"
    #     else :
    #         result['Strength_analysis'].loc[i]="Bearish"
    # result=result[["stock","Trend_analysis","RS_analysis","Strength_analysis","52W high","mscore","date","close","ST","closeh","STh","NIFTY 50","closenf","STnf","closenfh","STnfh","sector"
    #                ,"closesect","STsect","closesecth","STsecth","Sector_RS","Bench_RS","MACD_S","MACD_B","52W_High","52W_Low","MACD","MACDh","RSI"]]
    # result.to_csv(r"C:\Users\mange\OneDrive\Dashboard/"+dststock_path)
    # result = result.rename(columns={'52W high': 'one_year_high', 'NIFTY 50': 'NIFTY_50','52W_High':'one_year_high_abs','52W_Low':'one_year_low_abs'})
    # result.to_sql(db_name,engine,if_exists='replace')
def getsectorresult():
    sectors=pd.read_csv(r"")
    result=pd.DataFrame(columns=['sector', 'date', 'close', 'ST', 'MACD', 'closeh', 'STh', 'MACDh',
       'NIFTY 50', 'closenf', 'STnf', 'MACDnf', 'closenfh', 'STnfh', 'MACDnfh',
       'Bench_RS', 'MACD_B', 'RSI'])
    nifty=getdata("NIFTY 50",True)
    nifty.columns=["date","closenf","STnf","MACDnf","closenfh","STnfh","MACDnfh"]
    for sector in sectors['INDEX']:
        sectordata=getdata(sector,True)
        sectordata.insert(0,"sector",sector)
        sectordata.columns=['sector','date','close','ST','MACD','closeh','STh','MACDh']
        sectordata['NIFTY 50']='NIFTY 50'
        sectordata=pd.merge(sectordata,nifty,on=['date'],how='outer')
        sectordata["Bench_RS"]=sectordata['close']/sectordata['closenf']
        a,b,sectordata['MACD_B']=talib.MACD(sectordata["Bench_RS"],12,26,9)
        sectordata['RSI']=talib.RSI(sectordata["close"],14)
        sectordata['52W_High']=max(sectordata.tail(250)['close'])
        result=result.append(sectordata.iloc[-1])
    result=result.reset_index(drop=True)
    result['Trend_analysis']='NA'
    result['RS_analysis']='NA'
    result['Strength_Analysis']='NA'
    result['52W high']=(result["close"]-result['52W_High'])/result['52W_High']*100
    for i in range(0,len(result)):
        if result['close'].loc[i] > result['ST'].loc[i] and result['close'].loc[i] > result['STh'].loc[i]:
            result['Trend_analysis'].loc[i]="Bullish"
        else:
            result['Trend_analysis'].loc[i]="Bearish"
        if result['MACD_B'].loc[i] > 0:
            result['RS_analysis'].loc[i]="Bullish"
        else:
            result['RS_analysis'].loc[i]="Bearish"
        if result['MACDh'].loc[i] > 0:
            result['Strength_Analysis'].loc[i]="Bullish"
        else :
            result['Strength_Analysis'].loc[i]="Bearish"
    result.to_csv(r"")
    

def bullishlist():
    scripts=pd.read_csv("new_scripts.csv",index_col=0)
    bull=[]
    bear=[]
    bullist=scripts['Bullish_scripts'].tolist()
    bearlist=scripts['Bearish_scripts'].tolist()
    result=pd.read_csv("RS_dashboard.csv",index_col=0)
    for i in range(0,len(result['stock'])):
        if result['RS_analysis'].loc[i]=='Bullish' and (result['stock'].loc[i] not in bullist ):
            bull.append(result['stock'].loc[i])
        else:
            if result['RS_analysis'].loc[i]=='Bearish' and (result['stock'].loc[i] not in bearlist):
                bear.append(result['stock'].loc[i])
    scripts['Bullish_scripts']=pd.Series(bull)
    scripts['Bearish_scripts']=pd.Series(bear)
    scripts.to_csv('new_scripts.csv')


tic=dt.datetime.now()
getstockresult(r"","RS_dashboard.csv",'stock_screener')
getstockresult(r"","MidCap.csv",'Midcap')
getstockresult(r"","NIFTY_DASHBOARD.csv",'nifty_50')
playsound(r"")
getsectorresult()
toc=dt.datetime.now()
print(toc-tic)
playsound(r"")
