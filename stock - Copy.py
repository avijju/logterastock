import time
import ast
import sqlite3
import pandas as pd
from typing import List
from polygon import WebSocketClient, STOCKS_CLUSTER, CRYPTO_CLUSTER, FOREX_CLUSTER
from sqlalchemy import create_engine
import mysql.connector as msql

def my_custom_process_message(messages: List[str]):
    
    def add_message_to_list(message):
        
        messages.append(ast.literal_eval(message))

    return add_message_to_list


def main(waiting_time = 30):
    
    key = 'DJwpTfnSeTL0T9Ie3nPHjpwd466R3WlM'
    messages = []
   
    
    my_client = WebSocketClient(STOCKS_CLUSTER, key, my_custom_process_message(messages))  
    my_client.run_async() 
       
    my_client.subscribe("AM.AAPL,AM.MSFT,AM.GOOG,AM.FB,AM.NFLX,AM.TSLA,AM.DIS,AM.AMD,AM.INTC,AM.BA")  # Stock data
    time.sleep(waiting_time)

    my_client.close_connection()
    #print(messages)
    df = pd.DataFrame(messages)

    df = df.iloc[5:, 0].to_frame()
    df.columns = ["data"]
    df["data"] = df["data"].astype("str")

    df = pd.json_normalize(df["data"].apply(lambda x : dict(eval(x))))
    if df.empty:
        print('DataFrame is empty!')
    else:
        print(df)
          
        connect_string = 'mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4'.format("avi", "logtera", "5.189.178.77",  "Stock")
    
        engine = create_engine(connect_string)
    
        df.to_sql("aggregates", engine, if_exists='append', index=False)
    #else:
    print("Finally finished!")

if __name__ == "__main__":
    main()