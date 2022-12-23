import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
from pandas import DataFrame
from datetime import datetime, timedelta
import mysql.connector
from urllib.parse import quote_plus
from sqlalchemy.engine import create_engine
import profilehooks
from datetime import date

chrome_path = r"C:\Users\Jawahar\Desktop\chromedriver_win32\chromedriver.exe"
s = Service(chrome_path)
driver = webdriver.Chrome(service=s)

engine = create_engine("mysql://%s:Tcs#1234@uaa-db.mysql.database.azure.com:3306/dt_retail" % quote_plus("wadmin@uaa-db"))

mydb = mysql.connector.connect(
	host = "uaa-db.mysql.database.azure.com",
	user = "wadmin@uaa-db",
	password = ("Tcs#1234") 
 )

mydbcursor = mydb.cursor()
mydbcursor.execute("show databases")

def getMetrics():
	# read in your SQL query results using pandas
	metricsdf = pd.read_sql("""
        SELECT metric_id, metric_name, level, parent, metric_type
        FROM dt_retail.metrics_tbl
        ORDER BY metric_id
        """, engine)
	return metricsdf

mtricsdf = getMetrics()


def string_to_int(s):
    if not s:
        return s
    try:
        f = float(s)
        i = int(f)
        return i if f == i else f
    except ValueError:
        return s
    

def conStrToDateTime(datetime_str):
    try:
        datetime_object = datetime.strptime(datetime_str, '%m/%d/%Y')
        return datetime_object
        
    except ValueError as ve:    
        print('ValueError Raised:', ve)
        
def getQtrFromDt(datetimeObj):

    qtrOfDate = f'Q{(datetimeObj.month-1)//3+1}'
    return qtrOfDate

def getYearFromDt(datetimeObj):

    yearOfDate = datetimeObj.year
    return yearOfDate  


        
def get_data_from_yahooFinance(url_link,ticker):
    driver.get(url_link)
    time.sleep(2)

    #For hiding the popup coming to the screen
    if(len(driver.find_elements(By.XPATH,"//button[text()='Maybe later']")) > 0):
        hide_popup=driver.find_element(By.XPATH,"//button[text()='Maybe later']")
        ActionChains(driver).click(hide_popup).perform()
    
    #For expanding all columns
    expand=driver.find_element(By.XPATH,"//span[text()='Expand All']")
    ActionChains(driver).click(expand).perform()

    WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//div[@class="D(tbhg)"]')))

    headers_elem = driver.find_elements(By.XPATH, '//div[@class="D(tbhg)"]/div/div')
    col_headers = [header.text for header in headers_elem]
    

    col_headers_copy = []
    for i in range(len(col_headers)):

        if((col_headers[i] == 'Breakdown') or (col_headers[i] == 'TTM')):
            col_headers_copy.append(col_headers[i])
        else:
            dttime = conStrToDateTime(col_headers[i])
            headerDate = str(getYearFromDt(dttime)) + " " + getQtrFromDt(dttime)
            col_headers_copy.append(headerDate)
    df = pd.DataFrame(columns = col_headers_copy)

    rows = driver.find_element(By.XPATH, '//div[@class="D(tbrg)"]')
    rows_text=[row_value for row_value in rows.text.split("\n")]
    for i in range(len(rows_text)):
        if(i%2 != 0):
            row_data=[]
            row_data.append(rows_text[i-1])
            row_data+=rows_text[i].split(" ")
            
            new_row_data = []
            for i in row_data:
                data = i.replace(',',"")
                if len(i) == 1:
                    data = i.replace('-',"0")
                data1 = string_to_int(data)
                new_row_data.append(data1)
            df.loc[len(df)] = new_row_data  
    df['Ticker'] = ticker
    df.set_index("Breakdown")
    first_column = df.pop('Ticker')
    df.insert(0, 'Ticker', first_column)
    print(df)
    return df

def createMetYearlydataFrame():
    Edf = pd.DataFrame(columns=['metric_id','metric_name','metric_year','metric_value',
                                    'company_code','parent_metric_id','metric_level',"created_by","updated_by"])
    Edf.to_excel('EmptyYrlyDataFrame.xlsx')
    return Edf


@profilehooks.timecall
def populateYrlyStaging(met_data_yearly_staging_tbl, edf):
    truncateYrlyStagingTbl()
    edf.to_sql(met_data_yearly_staging_tbl, con=engine, schema='dt_retail', if_exists='append', index=False)
        # print("---checking edf ---")
        # print(edf)

@profilehooks.timecall
def truncateYrlyStagingTbl():
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""truncate table met_data_yearly_staging_tbl;""")
    except:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()



@profilehooks.timecall
def mergeYrlyDataIntotbl(table_name):
    table1= 'is_yearly_data'
    table2 = 'bs_yearly_data'
    table3 = 'cf_yearly_data'
    conn = engine.raw_connection()
    if (table_name == table1):
        try:
            with conn.cursor() as cur:
                print('in the iff for income_statement')
                cur.execute("""replace INTO income_statement_yearly_tbl (metric_id, metric_name, metric_year, metric_value, 
                    company_code, parent_metric_id, metric_level, created_by, updated_by)
                    SELECT metric_id, metric_name, metric_year, metric_value, company_code, parent_metric_id, metric_level, created_by, updated_by
                    FROM met_data_yearly_staging_tbl;""")
        except:
            conn.rollback()
            raise Exception("Sorry, insert into income_statement_yearly_tbl failed. Rolling back")
        else:
            conn.commit()
        finally:
            conn.close()
            
            
    elif (table_name == table2):
        #conn = engine.raw_connection()
        print('in the iff for Balance sheet')
        try:
            with conn.cursor() as cur:
                cur.execute("""replace INTO balance_sheet_yearly_tbl (metric_id, metric_name, metric_year, metric_value, 
                    company_code, parent_metric_id, metric_level, created_by, updated_by)
                    SELECT metric_id, metric_name, metric_year, metric_value, company_code, parent_metric_id, metric_level, created_by, updated_by
                    FROM met_data_yearly_staging_tbl;""")
        except:
            conn.rollback()
            raise Exception("Sorry, insert into balance_sheet_yearly_tbl failed. Rolling back")
        else:
            conn.commit()
        finally:
            conn.close()
            
    else:
        ##table_name == table3
        #conn = engine.raw_connection()
        print('in the iff for cash flow')
        try:
            with conn.cursor() as cur:
                cur.execute("""replace INTO cash_flow_yearly_tbl (metric_id, metric_name, metric_year, metric_value, 
                    company_code, parent_metric_id, metric_level, created_by, updated_by)
                    SELECT metric_id, metric_name, metric_year, metric_value, company_code, parent_metric_id, metric_level, created_by, updated_by
                    FROM met_data_yearly_staging_tbl;""")
        except:
            conn.rollback()
            raise Exception("Sorry, insert into cash_flow_yearly_tbl failed. Rolling back")
        else:
            conn.commit()
        finally:
            conn.close()
        
        
    #return table_name




@profilehooks.timecall
def createYrlyISdatabase(df):
    columns = df.columns.tolist()
    tickerid = ""
    iSYrlyDf = createMetYearlydataFrame()
    for _, i in df.iterrows():
        for c in columns:
            if(c == "Breakdown"):
                metricName = i[c]
            if(c == "Ticker"):
                tickerid = i[c]
            if(False == ((c == "Breakdown") or (c == "TTM") or (c == "Ticker"))):
                yearDate = c.split(" ")[0]
                quarter = c.split(" ")[1]
                metricDtlsrow = mtricsdf.loc[mtricsdf['metric_name'] == metricName]
                if (metricDtlsrow.empty):
                    continue
                m_id = metricDtlsrow['metric_id'].values[0]
                m_name = metricDtlsrow['metric_name'].values[0]
                m_level = metricDtlsrow['level'].values[0]
                m_parent = metricDtlsrow['parent'].values[0]
                m_type = metricDtlsrow['metric_type'].values[0]
                new_row = {'metric_id': str(m_id), 'metric_name':metricName, 'metric_year':yearDate, 
                            'metric_value':str(i[c]).replace(',',"") , 'company_code':tickerid, 'parent_metric_id':str(m_parent),
                           'metric_level':str(m_level), 'created_by':'"user1"', 'updated_by':'"user1"'}
                iSYrlyDf.loc[len(iSYrlyDf.index)] = new_row
    print("--- isYrlyDf ---")
    print(iSYrlyDf)
    populateYrlyStaging("met_data_yearly_staging_tbl", iSYrlyDf)
    mergeYrlyDataIntotbl("is_yearly_data")
    iSYrlyDf.to_excel("iS_yearlydata.xlsx")

@profilehooks.timecall
def createYrlyBSdatabase(df):
    columns = df.columns.tolist()
    tickerid = ""
    bSYrlyDf = createMetYearlydataFrame()
    for _, i in df.iterrows():
        for c in columns:
            if(c == "Breakdown"):
                metricName = i[c]
            if(c == "Ticker"):
                tickerid = i[c]
            if(False == ((c == "Breakdown") or (c == "TTM") or (c == "Ticker"))):
                yearDate = c.split(" ")[0]
                quarter = c.split(" ")[1]
                metricDtlsrow = mtricsdf.loc[mtricsdf['metric_name'] == metricName]
                if (metricDtlsrow.empty):
                    continue
                m_id = metricDtlsrow['metric_id'].values[0]
                m_name = metricDtlsrow['metric_name'].values[0]
                m_level = metricDtlsrow['level'].values[0]
                m_parent = metricDtlsrow['parent'].values[0]
                m_type = metricDtlsrow['metric_type'].values[0]
                new_row = {'metric_id': str(m_id), 'metric_name':metricName, 'metric_year':yearDate,
                           'metric_value':str(i[c]).replace(',',"") , 'company_code':tickerid, 'parent_metric_id':str(m_parent),
                           'metric_level':str(m_level), 'created_by':'"user1"', 'updated_by':'"user1"'}
                bSYrlyDf.loc[len(bSYrlyDf.index)] = new_row
    print("--- bSYrlyDf ---")
    print(bSYrlyDf)
    populateYrlyStaging("met_data_yearly_staging_tbl", bSYrlyDf)
    mergeYrlyDataIntotbl("bs_yearly_data")
    bSYrlyDf.to_excel("bs_yearlydata.xlsx")
    
    


@profilehooks.timecall
def createYrlyCFdatabase(df):
    columns = df.columns.tolist()
    tickerid = ""
    cFYrlyDf = createMetYearlydataFrame()
    for _, i in df.iterrows():
        for c in columns:
            if(c == "Breakdown"):
                metricName = i[c]
            if(c == "Ticker"):
                tickerid = i[c]
            if(False == ((c == "Breakdown") or (c == "TTM") or (c == "Ticker"))):
                yearDate = c.split(" ")[0]
                quarter = c.split(" ")[1]
                metricDtlsrow = mtricsdf.loc[mtricsdf['metric_name'] == metricName]
                if (metricDtlsrow.empty):
                    continue
                m_id = metricDtlsrow['metric_id'].values[0]
                m_name = metricDtlsrow['metric_name'].values[0]
                m_level = metricDtlsrow['level'].values[0]
                m_parent = metricDtlsrow['parent'].values[0]
                m_type = metricDtlsrow['metric_type'].values[0]
                new_row = {'metric_id': str(m_id), 'metric_name':metricName, 'metric_year':yearDate,
                            'metric_value':str(i[c]).replace(',',"") , 'company_code':tickerid, 'parent_metric_id':str(m_parent),
                           'metric_level':str(m_level), 'created_by':'"user1"', 'updated_by':'"user1"'}
                cFYrlyDf.loc[len(cFYrlyDf.index)] = new_row
    print("--- cfYrlyDf ---")
    print(cFYrlyDf)
    populateYrlyStaging("met_data_yearly_staging_tbl", cFYrlyDf)
    mergeYrlyDataIntotbl("cf_yearly_data")    
    cFYrlyDf.to_excel("cf_yearlydata.xlsx")
    

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



def write_to_excel(df, filename):
    df.to_excel(filename)


def get_income_statement_data():
    list_of_tickers=get_tickers_lists()
    df1=pd.DataFrame()
    for ticker in list_of_tickers:
        income_statement_url=build_income_statement_url(ticker)
        df1=get_data_from_yahooFinance(income_statement_url,ticker)
        createYrlyISdatabase(df1)
       
 
def get_balance_sheet_data():
    list_of_tickers=get_tickers_lists()
    df1=pd.DataFrame()
    for ticker in list_of_tickers:
        balance_sheet_url=build_balance_sheet_url(ticker)
        df1=get_data_from_yahooFinance(balance_sheet_url,ticker)
        createYrlyBSdatabase(df1)
        
        
def get_cash_flow_data():
    list_of_tickers=get_tickers_lists()
    df1=pd.DataFrame()
    for ticker in list_of_tickers:
        cash_flow_url=build_cash_flow_url(ticker)
        df1=get_data_from_yahooFinance(cash_flow_url,ticker)
        createYrlyCFdatabase(df1)
        
        
        
        
def main():
    get_income_statement_data()
    print("---Completed-Income_statement_tbl---")
    get_balance_sheet_data()
    print("---Completed-balance_sheet_tbl---")
    get_cash_flow_data()
    print("---Completed-cash_flow_tbl---")

main()        



    
    