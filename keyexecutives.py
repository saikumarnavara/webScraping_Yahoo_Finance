from selenium import webdriver
from bs4 import BeautifulSoup
import mysql.connector
import pandas as pd
from inscriptis import get_text
from sqlalchemy.engine import create_engine
from urllib.parse import quote_plus
import re

connection = mysql.connector.connect(
	host = "uaa-db-migrated.mysql.database.azure.com",
	user = "wadmin",
	password = ("Tcs#1234") 
 )
def get_company_codes():
    sql_select_Query = "SELECT * FROM dt_retail.company_tbl WHERE stock_exchange != 'PRIVATE' and stock_exchange != 'BVL';"
    cursor = connection.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    company_codes = []
    for row in records:
        codes  = (row[0]).replace(" ","")
        company_codes.append(codes)
    return company_codes

def get_keyExecutiveData():
    tickers_list = get_company_codes()
    for ticker in tickers_list:
        ticker = ticker.replace(' ','')
        print(ticker)
        url = (f"https://finance.yahoo.com/quote/{ticker}/profile?p={ticker}")
        driver = webdriver.Chrome()
        driver.get(url)
        driver.implicitly_wait(5)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        titles= soup.find_all("td", class_="Ta(start)")
        list_A=[] 
        list_B=[]
        count=0 
        for i in titles:
            if count%2==0:
                list_A.append(i.get_text())
            else:
                list_B.append(i.get_text())
            count += 1
        dictionary = (dict(zip(list_A, list_B)))
        for name,title in dictionary.items():
            print(name + "<==>" + title)
            

get_keyExecutiveData()