import time
import ast
import sqlite3
import pandas as pd
from typing import List
from polygon import WebSocketClient, STOCKS_CLUSTER, CRYPTO_CLUSTER, FOREX_CLUSTER
from sqlalchemy import create_engine
import mysql.connector as msql

def my_custom_process_message(messages: List[str]):
    """
        Custom processing function for incoming streaming messages.
    """
    def add_message_to_list(message):
        """
            Simple function that parses dict objects from incoming message.
        """
        messages.append(ast.literal_eval(message))

    return add_message_to_list


def main(waiting_time = 1):
    """
        Main function which connects to live stream data, and saves incoming data over
        some pre-determined time in an sqlite database.
    """
    key = 'DJwpTfnSeTL0T9Ie3nPHjpwd466R3WlM'
    messages = []
   
    #my_client = WebSocketClient(CRYPTO_CLUSTER, key, my_custom_process_message(messages))
    #my_client = WebSocketClient(FOREX_CLUSTER, key, my_custom_process_message(messages))
    
    #db = msql.connect(host='34.136.65.150',user='avi', passwd='logtera',       db='Stock')

# This line is that you need
    #cursor = db.cursor()
    #cursor.execute("SELECT count(*) FROM stocksymbols ")
    #result = cursor.fetchall()
    # for row in result:
    #     count=row[0]
    # loopcount=count/100
    # for x in range(int(loopcount)):
    #     offsets=x*100
    #     print(offsets)
    #     query = ("SELECT  GROUP_CONCAT(CONCAT('""', Ticker, '""' )) FROM (SELECT Ticker FROM stocksymbols LIMIT 100 offset {} ) AS stock").format(offsets)   
    #     print(query) 
    #     cursor.execute(query)
    #for x in range(6):
        # result = cursor.fetchall()
        # for row in result:
        #     text=row[0]
        # print(text)
    my_client = WebSocketClient(STOCKS_CLUSTER, key, my_custom_process_message(messages))  
    my_client.run_async() 
       # my_client.subscribe(text)  # Stock data
    my_client.subscribe("A.*")  # Stock data
    #my_client.subscribe("XA.BTC-USD", "XA.ETH-USD", "XA.LTC-USD")  # Crypto data
    #my_client.subscribe("C.USD/CNH", "C.USD/EUR")  # Forex data
    time.sleep(waiting_time)

    my_client.close_connection()

    df = pd.DataFrame(messages)

    df = df.iloc[5:, 0].to_frame()
    df.columns = ["data"]
    df["data"] = df["data"].astype("str")

    df = pd.json_normalize(df["data"].apply(lambda x : dict(eval(x))))
    if df.empty:
        print('DataFrame is empty!')
    else:
        print(df)
          #  df = df[df.ev != 'status']
            #df.drop(["status"], inplace = True)
           # df=df[['ev', 'sym','i', 'x','p', 's','t', 'z']]
        connect_string = 'mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4'.format("avi", "logtera", "34.136.65.150",  "Stock")
    #with sqlite3.connect("realtime_crypto.sqlite") as conn:
        #df.to_sql("data", con=conn, if_exists="append", index=False)
        engine = create_engine(connect_string)
    #print(df)
    # export data to sqlite
    #with sqlite3.connect("realtime_crypto.sqlite") as conn:
        df.to_sql("aggregates", engine, if_exists='append', index=False)
    #else:
    print("Finally finished!")

if __name__ == "__main__":
    main()