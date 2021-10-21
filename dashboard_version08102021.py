# -*- coding: utf-8 -*-
"""
Created on Fri Oct  8 00:10:36 2021

@author: Mangesh

"""
import datetime as dt
import pandas as pd
import psycopg2
import time
import os
import numpy as np
import json
from playsound import playsound
from selenium import webdriver
from sqlalchemy import create_engine
from io import BytesIO
import requests
connection = psycopg2.connect(
            host="",
            database="",
            user="",
            password="")
engine = create_engine('')
with open(r"",'r',encoding = 'utf-8') as f:
    current_date=f.readline()
date = dt.datetime.strptime(current_date,'%Y-%m-%d').date()
date += dt.timedelta(days=1)
def getindexfile():
    print("indexfile")
    result = requests.get("https://archives.nseindia.com/content/indices/ind_close_all_"+date.strftime("%d%m%Y").
                          upper()+".csv",timeout=5)
    data = pd.read_csv(BytesIO(result.content), header=0, sep=',', quotechar='"')
    data.to_sql('stageindexfile',engine,'bhav_copy_raw',if_exists='append')
def getbhavcopy():
    print(date)
    print("bhavcopy")
    result = requests.get("https://archives.nseindia.com/content/historical/EQUITIES/"+str(date.year)+"/"+date.strftime("%b").upper()+"/cm"+date.strftime("%d%b%Y").upper()+"bhav.csv.zip",timeout=5)
    df = pd.read_csv(BytesIO(result.content),compression='zip', header=0, sep=',', quotechar='"')
    df.to_sql('stagebhavcopy',engine,'bhav_copy_raw',if_exists='append')
while date <= dt.date.today():
    try:
        getbhavcopy()
        getindexfile()
    except:
        print('data not found for'+ str(date))
    date += dt.timedelta(days=1)
ddl_cursor=connection.cursor()
connection.rollback()
ddl_cursor.execute('ALTER TABLE bhav_copy_raw.stagebhavcopy ALTER COLUMN "TIMESTAMP" TYPE date USING "TIMESTAMP"::date;')
ddl_cursor.execute('ALTER TABLE bhav_copy_raw.stagebhavcopy DROP COLUMN "ISIN";')
ddl_cursor.execute('ALTER TABLE bhav_copy_raw.stagebhavcopy DROP COLUMN "Unnamed: 13";')
ddl_cursor.execute('delete from bhav_copy_raw.stageindexfile where '+'"Open Index Value"'+" = '-';")
ddl_cursor.execute('ALTER TABLE bhav_copy_raw.stageindexfile ALTER COLUMN "Index Date" TYPE date USING "Index Date"::date;')
ddl_cursor.execute('ALTER TABLE bhav_copy_raw.stageindexfile ALTER COLUMN "Open Index Value" TYPE float8 USING "Open Index Value"::float8;')
ddl_cursor.execute('ALTER TABLE bhav_copy_raw.stageindexfile ALTER COLUMN "High Index Value" TYPE float8 USING "High Index Value"::float8;')
ddl_cursor.execute('ALTER TABLE bhav_copy_raw.stageindexfile ALTER COLUMN "Low Index Value" TYPE float8 USING "Low Index Value"::float8;')
ddl_cursor.execute('insert into bhav_copy_raw.bhavcopy select * from bhav_copy_raw.stagebhavcopy ')
ddl_cursor.execute('insert into bhav_copy_raw.index_file select * from bhav_copy_raw.stageindexfile')
ddl_cursor.execute("delete from bhav_copy_raw.bhavcopy where"+ '"SERIES"'+" <> 'EQ';")
ddl_cursor.execute('update bhav_copy_raw.index_file set "Index Name"  = upper("Index Name" );')
connection.commit()
with open(r"",'w',encoding = 'utf-8') as text_file:
    text_file.write(str(dt.date.today()))

