from bs4 import BeautifulSoup as bs
import requests, re, json
import pandas as pd
import mysql.connector
from sqlalchemy.engine import create_engine
from urllib.parse import quote_plus



engine = create_engine("mysql://%s:Tcs#1234@uaa-db.mysql.database.azure.com:3306/dt_retail" % quote_plus("wadmin@uaa-db"))



connection = mysql.connector.connect(
    host = "uaa-db.mysql.database.azure.com",
    user = "wadmin@uaa-db",
    password = ("Tcs#1234") 
    )

def dataCounting(selectQry):
    #print(selectQry)
    #cnts = pd.read_sql('''SELECT COUNT(1) FROM dt_retail.cash_flow_tbl''',engine)
    cnts = pd.read_sql(selectQry, engine)
    count = cnts.values[0]
    
    return count

def insertData(insertQry, selectQry):
    
    # insert data into the Income Statement table
    #try:
    count = dataCounting(selectQry)
    # print("count ::: " + str(count) + str(type(count)) + str(type(int(count))) + str(int(count)) )
    if(int(count) == 0):
        # print(selectQry)
        engine.execute(insertQry)


def getData(ticker, compname, frequency):
    print(ticker)

    # r = requests.get(f'https://www.macrotrends.net/stocks/charts/{ticker}/{compname}/financial-ratios?freq={frequency}')
    r = requests.get(f'https://www.macrotrends.net/stocks/charts/M/macys/financial-ratios?freq=A')
    p = re.compile(r'var originalData = (.*);')
    p2 = re.compile(r'datafields:[\s\S]+(\[[\s\S]+?\]),')
    p3 = re.compile(r'\d{4}-\d{2}-\d{2}')
    data = json.loads(p.findall(r.text)[0])
    s = re.sub('\r|\n|\t|\s','',p2.findall(r.text)[0])
    fields = p3.findall(s)
    fields.insert(0, 'field_name') # only headers of interest.
    results = []

    for item in data: #loop initial list of dictionaries
        row = {}
        for f in fields: #loop keys of interest to extract from current dictionary
            if f == 'field_name':  #this is an html value field so needs re-parsing
                soup2 = bs(item[f],'lxml')
                row[f] = soup2.select_one('a,span').text
            else:
                row[f] = item[f]
        results.append(row)

    data = pd.DataFrame(results, columns = fields)
    data.to_excel("ratios.xlsx")
    get_ratio_data(data,ticker)  



def get_ratio_data(data,ticker):
    # sum_has_run = False
    ratioData = createMetdataFrame()
    columns = data.columns.tolist()
    
    for _, row in data.iterrows():
        for c in columns:
            metric_value = str(row[c])
            year = str(c).split("-")
            # j = (str(c[1:])+" "+ str(m))
        
            if (c == 'field_name'):
                metric_name = row[c]
                # print(metric_name)
            metricDtlsrow = mtricsdf.loc[mtricsdf['metric_name'] == metric_name]
            if (metricDtlsrow.empty):
                continue
            m_id = metricDtlsrow['metric_id'].values[0]
            m_name = metricDtlsrow['metric_name'].values[0]
            m_level = metricDtlsrow['level'].values[0]
            m_parent = metricDtlsrow['parent'].values[0]
            # print(m_name)
            new_row = {'metric_id': str(m_id), 'metric_name':metric_name, 'metric_year':year[0], 'metric_value':metric_value
                           , 'company_code':ticker, 'parent_metric_id':str(m_parent),
                           'metric_level':str(m_level), 'created_by':'"user1"', 'updated_by':'"user1"'}
            ratioData.loc[len(ratioData.index)] = new_row 
            # print(new_row) 
    print(ratioData)
    ratioData.to_excel("keyRatio.xlsx")

 
def createMetdataFrame():
    Edf = pd.DataFrame(columns=['metric_id','metric_name','metric_year','metric_value',
                                    'company_code','parent_metric_id','metric_level',"created_by","updated_by"])
    return Edf
  
def getMetrics():
	# read in your SQL query results using pandas
	metricsdf = pd.read_sql("""
        SELECT metric_id, metric_name, level, parent, metric_type
        FROM dt_retail.metrics_tbl
        ORDER BY metric_id
        """, engine)
	return metricsdf

mtricsdf = getMetrics()


    

def get_company_codes():
    sql_select_Query = "SELECT * FROM dt_retail.company_tbl;"
    cursor = connection.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    company_codes = []
    for row in records:
        company_codes.append(row)
    return company_codes


def main():
    list_of_tickers = get_company_codes()
    for tkr in list_of_tickers:
        ticker = tkr[0]
        company_name = tkr[1]
    getData( "M", 'macys' , 'A')
        # getData(ticker, company_name, 'A')
        # getData(ticker, company_name, 'Q')
        
        
main()






# is_insert_1stprt = "insert into dt_retail.income_statement_tbl (metric_id, metric_name, metric_year, metric_quarter, metric_value, company_code, parent_metric_id, metric_level, created_by, updated_by) values ("
# is_insert_2ndprt = str(m_id) + ', "' + metricName + '", ' + yearDate + ', "' +  quarter + '", ' + str(i[c]).replace(',',"") + ', "' + tickerid + '", ' + str(m_parent) + ', ' + str(m_level) + ', '
# is_insert_3rdprt = '"user1"' + ', ' + '"user1"' + ' );'
# is_insert_query = is_insert_1stprt + is_insert_2ndprt + is_insert_3rdprt
# isSelectQry_prt1 = "select count(1) from dt_retail.income_statement_tbl" 
# isSelectQry_prt2 = " where metric_year = " + yearDate 
# isSelectQry_prt3 = " and metric_quarter = '" + quarter + "'"
# isSelectQry_prt4 = " and metric_id = " + str(m_id)
# isSelectQry_prt5 = " and company_code = '" + tickerid + "';"
# isSelectQry = isSelectQry_prt1 + isSelectQry_prt2 + isSelectQry_prt3 + isSelectQry_prt4 + isSelectQry_prt5
# # count = dataCounting(isSelectQry)
# # print(" count : "  + str(count))
# insertISMetricData(is_insert_query, isSelectQry)
   