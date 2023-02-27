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

# Creating connection string 
#credentials = "mysql://wadmin@uaa-db:%s@uaa-db.mysql.database.azure.com:3306"
# engine = create_engine("mysql://%s:Tcs#1234@uaa-db.mysql.database.azure.com:3306/dt_retail" % quote_plus("wadmin@uaa-db"))
# connection = mysql.connector.connect(
#     host = "uaa-db.mysql.database.azure.com",
#     user = "wadmin@uaa-db",
#     password = ("Tcs#1234") 
#     )

engine = create_engine("mysql://%s:Tcs#1234@uaa-db-migrated.mysql.database.azure.com:3306/dt_retail" % quote_plus("wadmin"))

connection = mysql.connector.connect(
	host = "uaa-db-migrated.mysql.database.azure.com",
	user = "wadmin",
	password = ("Tcs#1234"),
    database ="dt_retail"
 )

# Creating connection object
# mydb = mysql.connector.connect(
# 	host = "uaa-db.mysql.database.azure.com",
# 	user = "wadmin@uaa-db",
# 	password = ("Tcs#1234") 
#  )

# mydbcursor = mydb.cursor()
# mydbcursor.execute("show databases")


def getCompanyCodes():
	# read in your SQL query results using pandas
	comcodedf = pd.read_sql("""
        SELECT company_code, company_name
        FROM dt_retail.company_tbl
        ORDER BY company_code
        """, engine)
	return comcodedf


def getQuarter():
	# read in your SQL query results using pandas
	quarterdf = pd.read_sql("""
        SELECT quarter_code, quarter_name
        FROM dt_retail.quarter_tbl
        ORDER BY quarter_code
        """, engine)
	return quarterdf

qtrdf = getQuarter()

def getMetrics():
	# read in your SQL query results using pandas
	metricsdf = pd.read_sql("""
        SELECT metric_id, metric_name, level, parent, metric_type
        FROM dt_retail.metrics_tbl
        ORDER BY metric_id
        """, engine)
	return metricsdf

mtricsdf = getMetrics()


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

    #Quarterly
    quarter=driver.find_element(By.XPATH,"//span[text()='Quarterly']")
    ActionChains(driver).click(quarter).perform()
    time.sleep(4)
    
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
    df = df.loc[:, ~df.columns.duplicated()]
    print(df)
    return df


def createMetdataFrame():
    Edf = pd.DataFrame(columns=['metric_id','metric_name','metric_year','metric_quarter','metric_value',
                                    'company_code','parent_metric_id','metric_level',"created_by","updated_by"])
    Edf.to_excel('EmptyDataFrame.xlsx')
    return Edf


@profilehooks.timecall
def populateStaging(met_data_staging_tbl, edf):
    truncateStagingTbl()
    edf.to_sql(met_data_staging_tbl, con=engine, schema='dt_retail', if_exists='append', index=False)
    # print("---checking edf ---")
    # print(edf)


@profilehooks.timecall
def truncateStagingTbl():
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""truncate table met_data_staging_tbl;""")
    except:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()
            

@profilehooks.timecall
def mergeDataIntotbl(table_name):
    table1= 'income_statement_tbl'
    table2 = 'balance_sheet_tbl'
    table3 = 'cash_flow_tbl'
    conn = engine.raw_connection()
    if (table_name == table1):
        try:
            with conn.cursor() as cur:
                print('in the iff for income_statement')
                cur.execute("""replace INTO income_statement_tbl (metric_id, metric_name, metric_year, metric_quarter, metric_value, 
                    company_code, parent_metric_id, metric_level, created_by, updated_by)
                    SELECT metric_id, metric_name, metric_year, metric_quarter, metric_value, company_code, parent_metric_id, metric_level, created_by, updated_by
                    FROM met_data_staging_tbl;""")
        except:
            conn.rollback()
            raise Exception("Sorry, insert into income_statement_tbl failed. Rolling back")
        else:
            conn.commit()
        finally:
            conn.close()
            
            
    elif (table_name == table2):
        #conn = engine.raw_connection()
        print('in the iff for Balance sheet')
        try:
            with conn.cursor() as cur:
                cur.execute("""replace INTO balance_sheet_tbl (metric_id, metric_name, metric_year, metric_quarter, metric_value, 
                    company_code, parent_metric_id, metric_level, created_by, updated_by)
                    SELECT metric_id, metric_name, metric_year, metric_quarter, metric_value, company_code, parent_metric_id, metric_level, created_by, updated_by
                    FROM met_data_staging_tbl;""")
        except:
            conn.rollback()
            raise Exception("Sorry, insert into balance_sheet_tbl failed. Rolling back")
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
                cur.execute("""replace INTO cash_flow_tbl (metric_id, metric_name, metric_year, metric_quarter, metric_value, 
                    company_code, parent_metric_id, metric_level, created_by, updated_by)
                    SELECT metric_id, metric_name, metric_year, metric_quarter, metric_value, company_code, parent_metric_id, metric_level, created_by, updated_by
                    FROM met_data_staging_tbl;""")
        except:
            conn.rollback()
            raise Exception("Sorry, insert into cash_flow_tbl failed. Rolling back")
        else:
            conn.commit()
        finally:
            conn.close()
        
        
    #return table_name



def string_to_int(s):
    if not s:
        return s
    try:
        f = float(s)
        i = int(f)
        return i if f == i else f
    except ValueError:
        return s
    
    
def get_tickers_lists():
    fh = open("tickers.txt") 
    tickers_string = fh.read()
    tickers_list = tickers_string.split("\n")
    tickers_list = list(filter(None,tickers_list))
    return tickers_list

# def get_tickers_lists():
#     sql_select_Query = "SELECT * FROM dt_retail.company_tbl WHERE stock_exchange != 'PRIVATE' and stock_exchange != 'BVL';"
#     cursor = connection.cursor()
#     cursor.execute(sql_select_Query)
#     records = cursor.fetchall()
#     company_codes = []
#     for row in records:
#         codes  = (row[0]).replace(" ","")
#         company_codes.append(codes)
#     return company_codes
    
def build_income_statement_url(ticker):
    url=f"https://finance.yahoo.com/quote/{ticker}/financials?p={ticker}"
    return url

def build_balance_sheet_url(ticker):
    url=f"https://finance.yahoo.com/quote/{ticker}/balance-sheet?p={ticker}"
    return url

def build_cash_flow_url(ticker):
    url=f"https://finance.yahoo.com/quote/{ticker}/cash-flow?p={ticker}"
    return url


@profilehooks.timecall
def createISdatabase(df):
    columns = df.columns.tolist()
    tickerid = ""
    iSmetDf = createMetdataFrame()
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
                new_row = {'metric_id': str(m_id), 'metric_name':metricName, 'metric_year':yearDate, 'metric_quarter':quarter
                           , 'metric_value':str(i[c]).replace(',',"") , 'company_code':tickerid, 'parent_metric_id':str(m_parent),
                           'metric_level':str(m_level), 'created_by':'"user1"', 'updated_by':'"user1"'}
                iSmetDf.loc[len(iSmetDf.index)] = new_row
    print("--- ismetDf ---")
    print(iSmetDf)
    populateStaging('met_data_staging_tbl', iSmetDf)
    mergeDataIntotbl('income_statement_tbl')

    


def createBSdatabase(df):
    columns = df.columns.tolist()
    tickerid = ""
    bSmetDf = createMetdataFrame()
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
                new_row = {'metric_id': str(m_id), 'metric_name':metricName, 'metric_year':yearDate, 'metric_quarter':quarter
                           , 'metric_value':str(i[c]).replace(',',"") , 'company_code':tickerid, 'parent_metric_id':str(m_parent),
                           'metric_level':str(m_level), 'created_by':'"user1"', 'updated_by':'"user1"'}
                bSmetDf.loc[len(bSmetDf.index)] = new_row
    print("--- bsmetDf ---")
    print(bSmetDf)
    bSmetDf.to_excel("ruff.xlsx")  
    populateStaging('met_data_staging_tbl', bSmetDf)
    mergeDataIntotbl('balance_sheet_tbl')

    


def createCFdatabase(df):
    columns = df.columns.tolist()
    tickerid = ""
    cFmetDf = createMetdataFrame()
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
                new_row = {'metric_id': str(m_id), 'metric_name':metricName, 'metric_year':yearDate, 'metric_quarter':quarter
                           , 'metric_value':str(i[c]).replace(',',"") , 'company_code':tickerid, 'parent_metric_id':str(m_parent),
                           'metric_level':str(m_level), 'created_by':'"user1"', 'updated_by':'"user1"'}
                cFmetDf.loc[len(cFmetDf.index)] = new_row
    print("--- cfmetDf ---")
    print(cFmetDf)
    populateStaging('met_data_staging_tbl', cFmetDf)
    mergeDataIntotbl('cash_flow_tbl')



#### TTM logic starts here ========================================================

def createTTMdataFrame():
    Edf1 = pd.DataFrame(columns=['metric_id','metric_name','metric_TTM_year','metric_TTM_quarter','metric_TTM_value',
                                    'company_code','parent_metric_id','metric_level',"created_by","updated_by"])
    return Edf1


@profilehooks.timecall
def populateStagingTTM(met_data_staging_ttm_tbl, edf):
        truncateStgingTTMtbl()
        edf.to_sql(met_data_staging_ttm_tbl, con=engine, schema='dt_retail', if_exists='append', index=False)
        print("---- just completed populateStagingTTM ----")


@profilehooks.timecall
def truncateStgingTTMtbl():
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""truncate table met_data_staging_ttm_tbl;""")
            print("---- Just trancated the met_data_staging_ttm_tbl  ---")
    except:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()
            

@profilehooks.timecall
def mergeDataIntoTTMtbl(table_name):
    table1= 'income_statement_ttm_tbl'
    table2 = 'balance_sheet_ttm_tbl'
    table3 = 'cash_flow_ttm_tbl'
    conn = engine.raw_connection()
    if (table_name == table1):
        try:
            with conn.cursor() as cur:
                print('in the iff for income_statement ttm')
                cur.execute("""replace INTO income_statement_ttm_tbl (metric_id, metric_name, metric_TTM_year, metric_TTM_quarter, metric_TTM_value, 
                    company_code, parent_metric_id, metric_level, created_by, updated_by)
                    SELECT metric_id, metric_name, metric_TTM_year, metric_TTM_quarter, metric_TTM_value, company_code, parent_metric_id, metric_level, created_by, updated_by
                    FROM met_data_staging_ttm_tbl;""")
        except:
            conn.rollback()
            raise Exception("Sorry, insert into income_statement_ttm_tbl failed. Rolling back")
        else:
            conn.commit()
        finally:
            conn.close()
            
            
    elif (table_name == table2):
        print('in the iff for Balance sheet ttm')
        try:
            with conn.cursor() as cur:
                cur.execute("""replace INTO balance_sheet_ttm_tbl (metric_id, metric_name, metric_TTM_year, metric_TTM_quarter, metric_TTM_value, 
                    company_code, parent_metric_id, metric_level, created_by, updated_by)
                    SELECT metric_id, metric_name, metric_TTM_year, metric_TTM_quarter, metric_TTM_value, company_code, parent_metric_id, metric_level, created_by, updated_by
                    FROM met_data_staging_ttm_tbl;""")
        except:
            conn.rollback()
            raise Exception("Sorry, insert into balance_sheet_ttm_tbl failed. Rolling back")
        else:
            conn.commit()
        finally:
            conn.close()
            
    else:
        table_name == table3
        print('in the iff for cash flow ttm')
        try:
            with conn.cursor() as cur:
                cur.execute("""replace INTO cash_flow_ttm_tbl (metric_id, metric_name, metric_TTM_year, metric_TTM_quarter, metric_TTM_value, 
                    company_code, parent_metric_id, metric_level, created_by, updated_by)
                    SELECT metric_id, metric_name, metric_TTM_year, metric_TTM_quarter, metric_TTM_value, company_code, parent_metric_id, metric_level, created_by, updated_by
                    FROM met_data_staging_ttm_tbl;""")
        except:
            conn.rollback()
            raise Exception("Sorry, insert into cash_flow_tbl failed. Rolling back")
        else:
            conn.commit()
        finally:
            conn.close()
        
        


@profilehooks.timecall
def createISTTMdatabase(df):
    columns = df.columns.tolist()
    tickerid = ""
    iSmetTTMDf = createTTMdataFrame()
    for _, i in df.iterrows():
        for c in columns:
            if(c == "Breakdown"):
                metricName = i[c]
            if(c == "Ticker"):
                tickerid = i[c]
            if((c == "TTM")):
                ttm = i[c]
            if (c == 'TTM') :
                #today  = date.today()
                # lastqtrtime = datetime.datetime.now() - timedelta(days=90)
                current_time = datetime.now() 
                headerDate = str(getYearFromDt(current_time)) + " " + getQtrFromDt(current_time)
                yearDate = headerDate.split(" ")[0]
                quarter = headerDate.split(" ")[1]
                metricDtlsrow = mtricsdf.loc[mtricsdf['metric_name'] == metricName]
                if (metricDtlsrow.empty):
                    continue
                m_id = metricDtlsrow['metric_id'].values[0]
                m_name = metricDtlsrow['metric_name'].values[0]
                m_level = metricDtlsrow['level'].values[0]
                m_parent = metricDtlsrow['parent'].values[0]
                m_type = metricDtlsrow['metric_type'].values[0]
                new_row = {'metric_id': str(m_id), 'metric_name':metricName, 'metric_TTM_year':yearDate, 'metric_TTM_quarter':quarter
                           , 'metric_TTM_value': str(ttm).replace(',',"") , 'company_code':tickerid, 'parent_metric_id':str(m_parent),
                           'metric_level':str(m_level), 'created_by':'"user1"', 'updated_by':'"user1"'}
                iSmetTTMDf.loc[len(iSmetTTMDf.index)] = new_row
    iSmetTTMDf.to_excel("is_ttm_data.xlsx")
    print("---IS-TTM-DATA--------")
    print(iSmetTTMDf)
    populateStagingTTM('met_data_staging_ttm_tbl', iSmetTTMDf)
    mergeDataIntoTTMtbl('income_statement_ttm_tbl')

    
    
@profilehooks.timecall
def createBSTTMdatabase(df):
    columns = df.columns.tolist()
    tickerid = ""
    bSmetTTMDf = createTTMdataFrame()
    for _, i in df.iterrows():
        for c in columns:
            if(c == "Breakdown"):
                metricName = i[c]
            if(c == "Ticker"):
                tickerid = i[c]
            if((c == "TTM")):
                current_time = datetime.now() 
                headerDate = str(getYearFromDt(current_time)) + " " + getQtrFromDt(current_time)
                yearDate = headerDate.split(" ")[0]
                quarter = headerDate.split(" ")[1]
                metricDtlsrow = mtricsdf.loc[mtricsdf['metric_name'] == metricName]
                if (metricDtlsrow.empty):
                    continue
                m_id = metricDtlsrow['metric_id'].values[0]
                m_name = metricDtlsrow['metric_name'].values[0]
                m_level = metricDtlsrow['level'].values[0]
                m_parent = metricDtlsrow['parent'].values[0]
                m_type = metricDtlsrow['metric_type'].values[0]
                new_row = {'metric_id': str(m_id), 'metric_name':metricName, 'metric_TTM_year':yearDate, 'metric_TTM_quarter':quarter
                           , 'metric_TTM_value':str(i[c]).replace(',',"") , 'company_code':tickerid, 'parent_metric_id':str(m_parent),
                           'metric_level':str(m_level), 'created_by':'"user1"', 'updated_by':'"user1"'}
                print(new_row)
                bSmetTTMDf.loc[len(bSmetTTMDf.index)] = new_row
    bSmetTTMDf.to_excel("bs_ttm_data.xlsx")
    print("---BS-TTM-DATA-----")
    print(bSmetTTMDf)
    populateStagingTTM('met_data_staging_ttm_tbl', bSmetTTMDf)
    mergeDataIntoTTMtbl('balance_sheet_ttm_tbl')
    
    
    
    
@profilehooks.timecall
def createCFTTMdatabase(df):
    columns = df.columns.tolist()
    tickerid = ""
    cFmetTTMDf = createTTMdataFrame()
    for _, i in df.iterrows():
        for c in columns:
            if(c == "Breakdown"):
                metricName = i[c]
            if(c == "Ticker"):
                tickerid = i[c]
            if((c == "TTM")):
                current_time = datetime.now() 
                headerDate = str(getYearFromDt(current_time)) + " " + getQtrFromDt(current_time)
                yearDate = headerDate.split(" ")[0]
                quarter = headerDate.split(" ")[1]
                metricDtlsrow = mtricsdf.loc[mtricsdf['metric_name'] == metricName]
                if (metricDtlsrow.empty):
                    continue
                m_id = metricDtlsrow['metric_id'].values[0]
                m_name = metricDtlsrow['metric_name'].values[0]
                m_level = metricDtlsrow['level'].values[0]
                m_parent = metricDtlsrow['parent'].values[0]
                m_type = metricDtlsrow['metric_type'].values[0]
                new_row = {'metric_id': str(m_id), 'metric_name':metricName, 'metric_TTM_year':yearDate, 'metric_TTM_quarter':quarter
                           , 'metric_TTM_value':str(i[c]).replace(',',"") , 'company_code':tickerid, 'parent_metric_id':str(m_parent),
                           'metric_level':str(m_level), 'created_by':'"user1"', 'updated_by':'"user1"'}
                cFmetTTMDf.loc[len(cFmetTTMDf.index)] = new_row
    cFmetTTMDf.to_excel("cf_ttm_data.xlsx")
    print("---CF-TTM-DATA----")
    print(cFmetTTMDf)
    populateStagingTTM('met_data_staging_ttm_tbl', cFmetTTMDf)
    mergeDataIntoTTMtbl('cash_flow_ttm_tbl')



### TTM logic ends here 
                
def write_to_excel(df, filename):
    df.to_excel(filename)


def get_income_statement_data():
    list_of_tickers=get_tickers_lists()
    df1=pd.DataFrame()
    for ticker in list_of_tickers:
        income_statement_url=build_income_statement_url(ticker)
        df1=get_data_from_yahooFinance(income_statement_url,ticker)
        df1.to_excel("new_is.xlsx")
        createISdatabase(df1)
        # createISTTMdatabase(df1)
        df1.drop(df1.index,inplace=True)
 
    
def get_balance_sheet_data():
    list_of_tickers=get_tickers_lists()
    df1=pd.DataFrame()
    for ticker in list_of_tickers:
        balance_sheet_url=build_balance_sheet_url(ticker)
        df1=get_data_from_yahooFinance(balance_sheet_url,ticker)
        df1.to_excel("new_bs.xlsx")
        createBSdatabase(df1)
        # createBSTTMdatabase(df1)
        df1.drop(df1.index,inplace=True)

       
def get_cash_flow_data():
    list_of_tickers=get_tickers_lists()
    df1=pd.DataFrame()
    for ticker in list_of_tickers:
        cash_flow_url=build_cash_flow_url(ticker)
        df1=get_data_from_yahooFinance(cash_flow_url,ticker)
        df1.to_excel("new_cf.xlsx")
        createCFdatabase(df1)
        # createCFTTMdatabase(df1)
        df1.drop(df1.index,inplace=True)

        
        
def main():
    get_income_statement_data()
    print("---Completed-Income_statement_tbl---")
    get_balance_sheet_data()
    print("---Completed-balance_sheet_tbl---")
    get_cash_flow_data()
    print("---Completed-cash_flow_tbl---")

main()


    


