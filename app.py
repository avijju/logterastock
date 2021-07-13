import os
from flask import Flask, render_template, request, jsonify
import sqlalchemy
from werkzeug.utils import secure_filename
from werkzeug.datastructures import  FileStorage
import sys
import pandas as pd
import numpy as np
import dateutil.parser as DP
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import seaborn as sns
import matplotlib.pyplot as plt
import mysql.connector as msql
from mysql.connector import Error
import pymysql
import json
from django.core.serializers.json import DjangoJSONEncoder

from collections import namedtuple
from json import JSONEncoder
from datetime import datetime
from decimal import Decimal
from typing import Optional, List
import requests
import csv
import time
import ast
from polygon import WebSocketClient, STOCKS_CLUSTER, CRYPTO_CLUSTER, FOREX_CLUSTER

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 1024 * 1024
conn_params_dic = {
    "host"      : "5.189.178.77",
    "database"  : "dataana",
    "user"      : "root",
    "password"  : "logtera"
}
app.config['UPLOAD_PATH'] = 'uploads'
@app.route("/")
def main():
    return render_template('index.html')


@app.route('/upload')
def upload_file():
   return render_template('upload.html')
	
@app.route('/uploader', methods = ['POST'])
def upload_files():
   if request.method == 'POST':
      f = request.files['file']
      f.save(os.path.join(app.config['UPLOAD_PATH'], f.filename))
      rootPath = os.getcwd()
      rootPath=rootPath+"/uploads/"
      loc = (rootPath+f.filename)
     
      irisData = pd.read_csv(loc,index_col=False,parse_dates=['CreatedDate', 'ExpirationDate'] )
      
      
      irisData.head()
      df= irisData
      df.head()
      print(df.dtypes)
      execute_many(df,'iris')
      return 'file uploaded successfully'

def execute_many(datafrm, table):    
#     conn = msql.connect(host='5.189.178.77',
# database='dataana',
# user='root',
# password="logtera@1234")
    # Creating a list of tupples from the dataframe values
   
    tpls = [tuple(x) for x in datafrm.to_numpy()]    
    # dataframe columns with Comma-separated
    cols = ','.join(list(datafrm.columns))  
    print(cols)  
   
    # SQL query to execute
    # sql = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % (table, cols)
    # cursor = conn.cursor()
    connect_string = 'mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4'.format("avi", "logtera", "5.189.178.77",  "dataana")
    # engine = create_engine(sqlalchemy.engine.url.URL.create(
    #     drivername="mysql+pymysql",
    #     username="avi",  # e.g. "my-database-user"
    #     password="logtera",  # e.g. "my-database-password"
    #     database="dataana",  # e.g. "my-database-name"
    #     query={
    #         "unix_socket": "{}/{}".format(
    #             "/cloudsql/",  # e.g. "/cloudsql"
    #             "dataanalysis-318005:us-central1:dataanalysis")  # i.e "<PROJECT-NAME>:<INSTANCE-REGION>:<INSTANCE-NAME>"
    #     }))
    engine = create_engine(connect_string)
    try:
        datafrm.to_sql("iris", engine, if_exists='replace', index=False)
        # cursor.executemany(sql, tpls)
        # conn.commit()
        print("Data inserted using execute_many() successfully...")
    except Error as e:
        print("Error", e)
        # cursor.close()
@app.route('/table')                                                                                  
def page_test():
    db = msql.connect(host='5.189.178.77',
                             user='avi',
                             passwd='logtera',
                             db='dataana')

# This line is that you need
    cursor = db.cursor(dictionary=True)

    print("i m called")
    cursor.execute("SELECT * FROM iris")

    result = cursor.fetchall()
    # enco = lambda obj: (
    # obj.isoformat()
    # if isinstance(obj, datetime.datetime)
    # or isinstance(obj, datetime.date)
    # else None
    # )
    return json.dumps(result, indent=4, sort_keys=True, default=str)


    
@app.route('/tables')
def tables():
   return render_template('table.html')
@app.route('/stockdata')                                                                                  
def stockdata():
    db = msql.connect(host='5.189.178.77',
                             user='avi',
                             passwd='logtera',
                             db='Stock')

# This line is that you need
    try:
        cursor = db.cursor(dictionary=True)

        print("i m called")
        #cursor.execute("SELECT ev,sym,v,av,op, truncate((vw*v),2) total,truncate(vw,2) vw,truncate(o,2) o,truncate(c,2) c,truncate(h,2) h,truncate(l,2) l,truncate(a,2) a,z,s,e,saveddate FROM stock.aggregates where saveddate =(select saveddate from Stock.aggregates ORDER BY  saveddate DESC lIMIT 1)")
        cursor.execute("with cte as(SELECT sym, o, l,v,saveddate, ROW_NUMBER() OVER (PARTITION BY sym ORDER BY saveddate DESC) as country_rank    FROM aggregates), cte1 as (select sym,o,saveddate from cte where country_rank = 1 and sym = sym), cte2 as (select sym,l,saveddate from cte where country_rank = 5 and sym = sym), cte3 as (select sum(v) sum,sym from cte where country_rank <= 5 and sym = sym group by sym) select cte1.sym,truncate(cte1.o,2) o,truncate(cte2.l,2) l,cte3.sum,truncate(((((cte1.o)-(cte2.l)))*cte3.sum),2) total,cte1.saveddate as odate,cte2.saveddate as ldate  from cte1 join cte2 on cte1.sym = cte2.sym join cte3 on cte1.sym = cte3.sym")
        result = cursor.fetchall()
        #print(result)
        enco = lambda obj: (
        obj.isoformat()
        if isinstance(obj, datetime.datetime)
        or isinstance(obj, datetime.date)
        else None
        )
        return json.dumps(result, indent=4, sort_keys=True, default=str)

    except Error as e:
        #print("Error")
        print(e)
    finally:
        cursor.close()
        db.close()
    
def json_dumps_default(obj):
    # ref: http://stackoverflow.com/a/16957370/2144390
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, datetime):
        return str(obj)
    raise TypeError

@app.route('/stocktables')
def stocktables():
   return render_template('stocktable.html')

def myconverter(o):
    if isinstance(o, datetime.date):
        return o.__str__()

@app.route('/stock')                                                                                  
def Get_Stock():
    # db = msql.connect(host='5.189.178.77',
    #                          user='avi',
    #                          passwd='logtera',
    #                          db='Stock')
    url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"

    querystring = {"tickers":"MSFT,AAPL,GOOG","apiKey":"DJwpTfnSeTL0T9Ie3nPHjpwd466R3WlM"}

    payload = "------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"Username\"\r\n\r\navinash.pandith@artha.app\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"Password\"\r\n\r\nMIPLinfo@1234\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--"
    headers = {
    'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
    'cache-control': "no-cache",
    'postman-token': "a206da2d-bb81-62de-3e9e-261f33c5fffc"
    }
    try:
        response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
       
        print("Data inserted using execute_many() successfully...")
    except Error as e:
        print("Error", e)
        # cursor.close()
   # jtopy=json.dump(response.json())
    #object_name = namedtuple("ObjectName", response.json().keys())(*response.json().values())
    #student = json.loads(str(response.json()), object_hook=Welcome6)
    stocks=Welcome6(response.json())
    aa= json.loads(json.dumps(response.json()), object_hook=Welcome6)
# This line is that you need
    # cursor = db.cursor(dictionary=True)

    # print("i m called")
    # cursor.execute("SELECT * FROM iris")

    # result = cursor.fetchall()
    # enco = lambda obj: (
    # obj.isoformat()
    # if isinstance(obj, datetime.datetime)
    # or isinstance(obj, datetime.date)
    # else None
    # )
    return str(aa)


    
class Day:
    c: float
    h: float
    l: float
    o: float
    v: int
    vw: float
    av: Optional[int]

    def __init__(self, c: float, h: float, l: float, o: float, v: int, vw: float, av: Optional[int]) -> None:
        self.c = c
        self.h = h
        self.l = l
        self.o = o
        self.v = v
        self.vw = vw
        self.av = av


class LastQuote:
    p: float
    s: int
    last_quote_p: float
    last_quote_s: int
    t: int

    def __init__(self, p: float, s: int, last_quote_p: float, last_quote_s: int, t: int) -> None:
        self.p = p
        self.s = s
        self.last_quote_p = last_quote_p
        self.last_quote_s = last_quote_s
        self.t = t


class LastTrade:
    c: List[int]
    i: str
    p: float
    s: int
    t: int
    x: int

    def __init__(self, c: List[int], i: str, p: float, s: int, t: int, x: int) -> None:
        self.c = c
        self.i = i
        self.p = p
        self.s = s
        self.t = t
        self.x = x


class Ticker:
    day: Day
    last_quote: LastQuote
    last_trade: LastTrade
    min: Day
    prev_day: Day
    ticker: str
    todays_change: float
    todays_change_perc: float
    updated: int

    def __init__(self, day: Day, last_quote: LastQuote, last_trade: LastTrade, min: Day, prev_day: Day, ticker: str, todays_change: float, todays_change_perc: float, updated: int) -> None:
        self.day = day
        self.last_quote = last_quote
        self.last_trade = last_trade
        self.min = min
        self.prev_day = prev_day
        self.ticker = ticker
        self.todays_change = todays_change
        self.todays_change_perc = todays_change_perc
        self.updated = updated


class Welcome6:
    status: str
    count: int
    tickers: List[Ticker]
    
    def __init__(self, status: str, count: int, tickers: List[Ticker]) -> None:
        self.status = status
        self.count = count
        self.tickers = tickers

if __name__ == "__main__":
    app.run()