import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd


chrome_path = r"C:\Users\Jawahar\Desktop\chromedriver_win32\chromedriver.exe"
s = Service(chrome_path)
driver = webdriver.Chrome(service=s)

def get_data_from_yahooFinance(url_link,ticker):
    driver.get(url_link)
    time.sleep(2)

    #For hiding the popup coming to the screen
    if(len(driver.find_elements(By.XPATH,"//button[text()='Maybe later']")) > 0):
        hide_popup=driver.find_element(By.XPATH,"//button[text()='Maybe later']")
        ActionChains(driver).click(hide_popup).perform()

    #Quarterly
    quarter=driver.find_element(By.XPATH,"//span[text()='Quarterly']")
    ActionChains(driver).click(quarter).perform()
    
    #For expanding all columns
    expand=driver.find_element(By.XPATH,"//span[text()='Expand All']")
    ActionChains(driver).click(expand).perform()

    WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//div[@class="D(tbhg)"]')))

    headers_elem = driver.find_elements(By.XPATH, '//div[@class="D(tbhg)"]/div/div')
    col_headers = [header.text for header in headers_elem]
    df = pd.DataFrame(columns = col_headers)

    rows = driver.find_element(By.XPATH, '//div[@class="D(tbrg)"]')
    rows_text=[row_value for row_value in rows.text.split("\n")]
    for i in range(len(rows_text)):
        if(i%2 != 0):
            row_data=[]
            row_data.append(rows_text[i-1])
            row_data+=rows_text[i].split(" ")
            df.loc[len(df)] = row_data
         
    df['Ticker'] = ticker
    return df
    
def get_tickers_lists():
    fh = open("tickers.txt") 
    tickers_string = fh.read()
    tickers_list = tickers_string.split("\n")
    tickers_list = list(filter(None,tickers_list))
    return tickers_list
    
def build_income_statement_url(ticker):
    url=f"https://finance.yahoo.com/quote/{ticker}/financials?p={ticker}"
    return url

def build_balance_sheet_url(ticker):
    url=f"https://finance.yahoo.com/quote/{ticker}/balance-sheet?p={ticker}"
    return url

def build_cash_flow_url(ticker):
    url=f"https://finance.yahoo.com/quote/{ticker}/cash-flow?p={ticker}"
    return url

def write_to_excel(df,filename):
    df.to_excel(filename)
    
def get_income_statement_data():
    list_of_tickers=get_tickers_lists()
    df=pd.DataFrame()
    for ticker in list_of_tickers:
        income_statement_url=build_income_statement_url(ticker)
        df1=get_data_from_yahooFinance(income_statement_url,ticker)
        df=pd.concat([df,df1],axis=0,ignore_index=True)
    #print(df)
    write_to_excel(df,'income_statement.xlsx')
    
def get_balance_sheet_data():
    list_of_tickers=get_tickers_lists()
    df=pd.DataFrame()
    for ticker in list_of_tickers:
        balance_sheet_url=build_balance_sheet_url(ticker)
        df1=get_data_from_yahooFinance(balance_sheet_url,ticker)
        df=pd.concat([df,df1],axis=0,ignore_index=True)
    #print(df)
    write_to_excel(df,'balance_sheet.xlsx')

def get_cash_flow_data():
    list_of_tickers=get_tickers_lists()
    df=pd.DataFrame()
    for ticker in list_of_tickers:
        cash_flow_url=build_cash_flow_url(ticker)
        df1=get_data_from_yahooFinance(cash_flow_url,ticker)
        df=pd.concat([df,df1],axis=0,ignore_index=True)
    #print(df)
    write_to_excel(df,'cash_flow.xlsx')
    
def main():
    get_income_statement_data()
    #get_balance_sheet_data()
    #get_cash_flow_data()
    print("--------Completed--------")
    
main()
    
    
    
        
    










