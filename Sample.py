import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
from pandas import DataFrame
from datetime import datetime
import mysql.connector
from urllib.parse import quote_plus
from sqlalchemy.engine import create_engine


chrome_path = r"C:\Users\Jawahar\Desktop\chromedriver_win32\chromedriver.exe"
s = Service(chrome_path)
driver = webdriver.Chrome(service=s)

# Creating connection string 
#credentials = "mysql://wadmin@uaa-db:%s@uaa-db.mysql.database.azure.com:3306"

engine = create_engine("mysql://%s:Tcs#1234@uaa-db.mysql.database.azure.com:3306" % quote_plus("wadmin@uaa-db"))

# Creating connection object
mydb = mysql.connector.connect(
	host = "uaa-db.mysql.database.azure.com",
	user = "wadmin@uaa-db",
	password = ("Tcs#1234") 
 )

mydbcursor = mydb.cursor()
mydbcursor.execute("show databases")
# for databases in mydbcursor:
#     # print(databases)

##mycccursor = mydb.cursor()
##ccode_data = mycccursor.execute("SELECT company_code, company_name FROM dt_retail.company_tbl ORDER BY company_code")
##comcodedf = DataFrame(ccode_data.fetchall())

#my_data = pd.read_sql("SELECT company_code, company_name FROM dt_retail.company_tbl ORDER BY company_code", mydb)
#print(my_data)


def getCompanyCodes():
	# read in your SQL query results using pandas
	comcodedf = pd.read_sql("""
        SELECT company_code, company_name
        FROM dt_retail.company_tbl
        ORDER BY company_code
        """, engine)
	return comcodedf

def dataCounting(selectQry):
    #print(selectQry)
    #cnts = pd.read_sql('''SELECT COUNT(1) FROM dt_retail.income_statement_tbl''',engine)
    cnts = pd.read_sql(selectQry, engine)
    count = cnts.values[0]
    return count

def dataCounting(selectQry):
    #print(selectQry)
    #cnts = pd.read_sql('''SELECT COUNT(1) FROM dt_retail.balance_sheet_tbl''',engine)
    cnts = pd.read_sql(selectQry, engine)
    count = cnts.values[0]
    return count


#Data counting for cash-flow-sheet
def dataCounting(selectQry):
    #print(selectQry)
    #cnts = pd.read_sql('''SELECT COUNT(1) FROM dt_retail.cash_flow_tbl''',engine)
    cnts = pd.read_sql(selectQry, engine)
    count = cnts.values[0]
    
    return count


def insertISMetricData(insertQry, selectQry):
    
    # insert data into the Income Statement table
    #try:
    count = dataCounting(selectQry)
    # print("count ::: " + str(count) + str(type(count)) + str(type(int(count))) + str(int(count)) )
    if(int(count) == 0):
        # print(selectQry)
        engine.execute(insertQry)
        
        
def insertBSMetricData(insertQry, selectQry):
    
    # insert data into the Income Statement table
    #try:
    count = dataCounting(selectQry)
    # print("count ::: " + str(count) + str(type(count)) + str(type(int(count))) + str(int(count)) )
    if(int(count) == 0):
        # print(selectQry)
        engine.execute(insertQry)


#inserting data for cash-flow-sheet
def insertCFMetricData(insertQry, selectQry):
    count = dataCounting(selectQry)
    # print("count ::: " + str(count) + str(type(count)) + str(type(int(count))) + str(int(count)) )
    if(int(count) == 0):
        # print(selectQry)
        engine.execute(insertQry)


def getQuarter():
	# read in your SQL query results using pandas
	quarterdf = pd.read_sql("""
        SELECT quarter_code, quarter_name
        FROM dt_retail.quarter_tbl
        ORDER BY quarter_code
        """, engine)
	return quarterdf

qtrdf = getQuarter()
# print(qtrdf)

def getMetrics():
	# read in your SQL query results using pandas
	metricsdf = pd.read_sql("""
        SELECT metric_id, metric_name, level, parent, metric_type
        FROM dt_retail.metrics_tbl
        ORDER BY metric_id
        """, engine)
	return metricsdf

mtricsdf = getMetrics()
# print(mtricsdf)



# Select rows where company_code is AAP
### print(ccdf.loc[ccdf['company_code'] == 'AAP'])

def conStrToDateTime(datetime_str):
    try:
        datetime_object = datetime.strptime(datetime_str, '%m/%d/%Y')
        # print(datetime_object)
        return datetime_object
        
    except ValueError as ve:    
        print('ValueError Raised:', ve)


def getQtrFromDt(datetimeObj):

    qtrOfDate = f'Q{(datetimeObj.month-1)//3+1}'
    ## qtrAppendQ = "Q" + qtrOfDate
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
        ##print(type(col_headers[i]))
        #if(i <= 1):
        if((col_headers[i] == 'Breakdown') or (col_headers[i] == 'TTM')):
            col_headers_copy.append(col_headers[i])
        #if(i > 1):
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
                data = i.replace('-',"0")
                data1 = string_to_int(data)
                #print(str(data1) + str(type(data1)))
                new_row_data.append(data1)
            # print(new_row_data) 
                
            df.loc[len(df)] = new_row_data  
    df['Ticker'] = ticker
    df.set_index("Breakdown")
    first_column = df.pop('Ticker')
    df.insert(0, 'Ticker', first_column)
    # Drop last column of a dataframe
    #df = df.iloc[: , :-1]
    # print("dasffffffffffff")
    return df

    
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
    
def build_income_statement_url(ticker):
    url=f"https://finance.yahoo.com/quote/{ticker}/financials?p={ticker}"
    return url

def build_balance_sheet_url(ticker):
    url=f"https://finance.yahoo.com/quote/{ticker}/balance-sheet?p={ticker}"
    return url

def build_cash_flow_url(ticker):
    url=f"https://finance.yahoo.com/quote/{ticker}/cash-flow?p={ticker}"
    return url

def createISdatabase(df):
    columns = df.columns.tolist()
    tickerid = ""
    for _, i in df.iterrows():
        
        for c in columns:
            ##print(i[c])
            ## print("column: " + str(c))
            if(c == "Breakdown"):
                metricName = i[c]
                ## get the metric data metric df
                
            if(c == "Ticker"):
                tickerid = i[c]
                # print("ticker : " + tickerid)
                                
            if(False == ((c == "Breakdown") or (c == "TTM") or (c == "Ticker"))):
                print("column: " + str(c))
                ##dateData.append(c.split(" "))
                ## print("dateData : " )
                ##print(dateData)
                yearDate = c.split(" ")[0]
                quarter = c.split(" ")[1]
            #   print("yeardate :" + yearDate)
            #   print("qter : " + quarter)
            #   print("met: " + metricName)
            #   insert into dt_retail.income_statement_tbl (metric_id, metric_name, metric_year, metric_quarter, metric_value, company_code, parent_metric_id, metric_level, created_by, updated_by)
            #   values (1, 'Total Revenue', 2022, 'Q4', 123455, 'AAP', 0, 1, 'JD', 'JD'); 
                is_insert_1stprt = "insert into dt_retail.income_statement_tbl (metric_id, metric_name, metric_year, metric_quarter, metric_value, company_code, parent_metric_id, metric_level, created_by, updated_by) values ("
            # print("############")
                # print("metricValue :" +  str(i[c]).replace(',',""))
                # print(mtricsdf.loc[mtricsdf['metric_name'] == metricName])
                metricDtlsrow = mtricsdf.loc[mtricsdf['metric_name'] == metricName]
                # metric_id metric_name  level  parent metric_type
                if (metricDtlsrow.empty):
                    continue
                m_id = metricDtlsrow['metric_id'].values[0]
                # print("m_id : " + str(m_id))
                m_name = metricDtlsrow['metric_name'].values[0]
                m_level = metricDtlsrow['level'].values[0]
                m_parent = metricDtlsrow['parent'].values[0]
                # print("metric level : " + str(m_level))
                m_type = metricDtlsrow['metric_type'].values[0]
                # print(str(m_id))
                # print(m_type)
                is_insert_2ndprt = str(m_id) + ', "' + metricName + '", ' + yearDate + ', "' +  quarter + '", ' + str(i[c]).replace(',',"") + ', "' + tickerid + '", ' + str(m_parent) + ', ' + str(m_level) + ', '
                # print('"user1"' + ', ' + '"user1"')
                is_insert_3rdprt = '"user1"' + ', ' + '"user1"' + ' );'
                is_insert_query = is_insert_1stprt + is_insert_2ndprt + is_insert_3rdprt
                # print(is_insert_query)
                
                isSelectQry_prt1 = "select count(1) from dt_retail.income_statement_tbl" 
                isSelectQry_prt2 = " where metric_year = " + yearDate 
                isSelectQry_prt3 = " and metric_quarter = '" + quarter + "'"
                isSelectQry_prt4 = " and metric_id = " + str(m_id)
                isSelectQry_prt5 = " and company_code = '" + tickerid + "';"
                isSelectQry = isSelectQry_prt1 + isSelectQry_prt2 + isSelectQry_prt3 + isSelectQry_prt4 + isSelectQry_prt5
                # count = dataCounting(isSelectQry)
                # print(" count : "  + str(count))
                insertISMetricData(is_insert_query, isSelectQry)
   






def createBSdatabase(df):
    columns = df.columns.tolist()
    tickerid = ""
    for _, i in df.iterrows():        
        for c in columns:
            print("column : " + str(c))
            if(c == "Breakdown"):
                metricName = i[c]
                print("new :" + metricName)
                ## get the metric data metric df
                
            if(c == "Ticker"):
                tickerid = i[c]
                # print("ticker : " + tickerid)
                                
            if(False == ((c == "Breakdown") or (c == "TTM") or (c == "Ticker"))):
                print("column: " + str(c))
                ##dateData.append(c.split(" "))
                ## print("dateData : " )
                ##print(dateData)
                yearDate = c.split(" ")[0]
                quarter = c.split(" ")[1]
            #   print("yeardate :" + yearDate)
            #   print("qter : " + quarter)
            #   print("met: " + metricName)
            #   insert into dt_retail.income_statement_tbl (metric_id, metric_name, metric_year, metric_quarter, metric_value, company_code, parent_metric_id, metric_level, created_by, updated_by)
            #   values (1, 'Total Revenue', 2022, 'Q4', 123455, 'AAP', 0, 1, 'JD', 'JD'); 
                bs_insert_1stprt = "insert into dt_retail.balance_sheet_tbl (metric_id, metric_name, metric_year, metric_quarter, metric_value, company_code, parent_metric_id, metric_level, created_by, updated_by) values ("
            # print("############")
                # print("metricValue :" +  str(i[c]).replace(',',""))
                # print(mtricsdf.loc[mtricsdf['metric_name'] == metricName])
                metricDtlsrow = mtricsdf.loc[mtricsdf['metric_name'] == metricName]
                # metric_id metric_name  level  parent metric_type
                if (metricDtlsrow.empty):
                    continue
                m_id = metricDtlsrow['metric_id'].values[0]
                # print("m_id : " + str(m_id))
                m_name = metricDtlsrow['metric_name'].values[0]
                m_level = metricDtlsrow['level'].values[0]
                m_parent = metricDtlsrow['parent'].values[0]
                # print("metric level : " + str(m_level))
                m_type = metricDtlsrow['metric_type'].values[0]
                # print(str(m_id))
                # print(m_type)
                bs_insert_2ndprt = str(m_id) + ', "' + metricName + '", ' + yearDate + ', "' +  quarter + '", ' + str(i[c]).replace(',',"") + ', "' + tickerid + '", ' + str(m_parent) + ', ' + str(m_level) + ', '
                print('"user1"' + ', ' + '"user1"')
                bs_insert_3rdprt = '"user1"' + ', ' + '"user1"' + ' );'
                bs_insert_query = bs_insert_1stprt + bs_insert_2ndprt + bs_insert_3rdprt
                # print(bs_insert_query)
                
                bsSelectQry_prt1 = "select count(1) from dt_retail.balance_sheet_tbl" 
                bsSelectQry_prt2 = " where metric_year = " + yearDate 
                bsSelectQry_prt3 = " and metric_quarter = '" + quarter + "'"
                bsSelectQry_prt4 = " and metric_id = " + str(m_id)
                bsSelectQry_prt5 = " and company_code = '" + tickerid + "';"
                bsSelectQry = bsSelectQry_prt1 + bsSelectQry_prt2 + bsSelectQry_prt3 + bsSelectQry_prt4 + bsSelectQry_prt5
                # count = dataCounting(isSelectQry)
                # print(" count : "  + str(count))
                insertISMetricData(bs_insert_query, bsSelectQry)
   







def createCFdatabase(df):
    columns = df.columns.tolist()
    tickerid = ""
    for _, i in df.iterrows():
        for c in columns:
            # print("value : " + str(i[c]))
            if(c == "Breakdown"):
                metricName = i[c]
                ## get the metric data metric df
                
            if(c == "Ticker"):
                tickerid = i[c]
                # print("ticker : " + tickerid)
                                
            if(False == ((c == "Breakdown") or (c == "TTM") or (c == "Ticker"))):
                # print("column: " + str(c))
                ##dateData.append(c.split(" "))
                ## print("dateData : " )
                ##print(dateData)
                yearDate = c.split(" ")[0]
                quarter = c.split(" ")[1]
            #   insert into dt_retail.income_statement_tbl (metric_id, metric_name, metric_year, metric_quarter, metric_value, company_code, parent_metric_id, metric_level, created_by, updated_by)
            #   values (1, 'Total Revenue', 2022, 'Q4', 123455, 'AAP', 0, 1, 'JD', 'JD'); 
                cf_insert_1stprt = "insert into dt_retail.cash_flow_tbl (metric_id, metric_name, metric_year, metric_quarter, metric_value, company_code, parent_metric_id, metric_level, created_by, updated_by) values ("
                # print("metricValue :" +  str(i[c]).replace(',',""))
                # print(mtricsdf.loc[mtricsdf['metric_name'] == metricName])
                metricDtlsrow = mtricsdf.loc[mtricsdf['metric_name'] == metricName]
                # metric_id metric_name  level  parent metric_type
                if (metricDtlsrow.empty):
                    continue
                m_id = metricDtlsrow['metric_id'].values[0]
                m_name = metricDtlsrow['metric_name'].values[0]
                m_level = metricDtlsrow['level'].values[0]
                m_parent = metricDtlsrow['parent'].values[0]
                # print("metric level : " + str(m_level))
                m_type = metricDtlsrow['metric_type'].values[0]
                # print(str(m_id))
                # print(" m_type " + str(m_type))
                cf_insert_2ndprt = str(m_id) + ', "' + metricName + '", ' + yearDate + ', "' +  quarter + '", ' + str(i[c]).replace(',',"") + ', "' + tickerid + '", ' + str(m_parent) + ', ' + str(m_level) + ', '
                # print(" ::::::;")
                # print(cf_insert_2ndprt)
                # print('"user1"' + ', ' + '"user1"')
                cf_insert_3rdprt = '"user1"' + ', ' + '"user1"' + ' );'
                cf_insert_query = cf_insert_1stprt + cf_insert_2ndprt + cf_insert_3rdprt
                # print(cf_insert_query)
                
                cfSelectQry_prt1 = "select count(1) from dt_retail.cash_flow_tbl" 
                cfSelectQry_prt2 = " where metric_year = " + yearDate 
                cfSelectQry_prt3 = " and metric_quarter = '" + quarter + "'"
                cfSelectQry_prt4 = " and metric_id = " + str(m_id)
                cfSelectQry_prt5 = " and company_code = '" + tickerid + "';"
                cfSelectQry = cfSelectQry_prt1 + cfSelectQry_prt2 + cfSelectQry_prt3 + cfSelectQry_prt4 + cfSelectQry_prt5
                # print(cfSelectQry)
                # count = dataCounting(isSelectQry)
                # print(" count : "  + str(count))
                insertCFMetricData(cf_insert_query, cfSelectQry)





# def createIsTTMdatabase(df):
#     columns = df.columns.tolist()
#     return columns

# aa = createIsTTMdatabase(df)
# print(aa)











                
def write_to_excel(df, filename):
    df.to_excel(filename)


def get_income_statement_data():
    list_of_tickers=get_tickers_lists()
    df1=pd.DataFrame()
    for ticker in list_of_tickers:
        income_statement_url=build_income_statement_url(ticker)
        df1=get_data_from_yahooFinance(income_statement_url,ticker)
        write_to_excel(df1,'INCOME_STATEMENT.xlsx')
        #df=pd.concat([df,df1],axis=0,ignore_index=True)
        print(df1)
        createISdatabase(df1)
        df1.drop(df1.index,inplace=True)
        # print("empty df1 :::") 
        # print(df1)
        
    #write_to_excel(df,'income_statement_sample.xlsx')
    #up_da_ter3(df)
    
def get_balance_sheet_data():
    list_of_tickers=get_tickers_lists()
    df1=pd.DataFrame()
    for ticker in list_of_tickers:
        balance_sheet_url=build_balance_sheet_url(ticker)
        df1=get_data_from_yahooFinance(balance_sheet_url,ticker)
        print(df1)
        createBSdatabase(df1)
        write_to_excel(df1,'balance_sheet.xlsx')
        df1.drop(df1.index,inplace=True)
        # print("empty df :::") 
        # print(df1)
        # df=pd.concat([df,df1],axis=0,ignore_index=True)
    # print(df)
    # write_to_excel(df,'balance_sheet.xlsx')
       
def get_cash_flow_data():
    list_of_tickers=get_tickers_lists()
    df1=pd.DataFrame()
    for ticker in list_of_tickers:
        cash_flow_url=build_cash_flow_url(ticker)
        df1=get_data_from_yahooFinance(cash_flow_url,ticker)
        print(df1)
        createCFdatabase(df1)
        write_to_excel(df1,'cash_flow.xlsx')
        df1.drop(df1.index,inplace=True)
        # print("empty df :::") 
        # print(df1)
        
        
def main():
    get_income_statement_data()
    get_balance_sheet_data()
    get_cash_flow_data()
    print("--------Completed--------")

main()

#bb = dataCounting()
#print("#############")
#print(type(bb))
# print(bb)
    
  
    
  #to replace (-) with blank      
  #string to datetime & 
  #date time conversion to year  & quarter

