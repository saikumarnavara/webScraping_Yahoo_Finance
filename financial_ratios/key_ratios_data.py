import requests
import json
import pandas as pd
def get_data(ticker):
    response_API = requests.get(F'https://fmpcloud.io/api/v3/ratios/{ticker}?limit=40&apikey=be347c8381d0ce379666ef3dcac74347')
    data = response_API.text
    key_ratio_data = json.loads(data)
    data= pd.DataFrame(key_ratio_data)
    txpdata = data.transpose()
    a = data.head()
    print(a)
    print(txpdata)
    return txpdata

def get_tickers_lists():
    fh = open("tickers.txt") 
    tickers_string = fh.read()
    tickers_list = tickers_string.split("\n")
    tickers_list = list(filter(None,tickers_list))
    return tickers_list

def get_ratio_data():
    list_of_tickers=get_tickers_lists()
    for ticker in list_of_tickers:
        get_data(ticker)
get_ratio_data()
print("--completed--")