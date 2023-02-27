from datetime import datetime, timedelta
from bs4 import BeautifulSoup as bs
import requests, re, json
import pandas as pd
import mysql.connector
from sqlalchemy.engine import create_engine
from urllib.parse import quote_plus
import profilehooks



def getData(ticker, compname, frequency):
    try:
        print(ticker)
        r = requests.get(f'https://www.macrotrends.net/stocks/charts/{ticker}/{compname}/financial-ratios?freq={frequency}')
        # r = requests.get(f'https://www.macrotrends.net/stocks/charts/M/macys/financial-ratios?freq=A')
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
        get_ratio_data(data,ticker,frequency)  
    except:
        print(ticker +" Is Invalid Ticker..!!!")

def get_ratio_data(data,ticker,frequency):
    annualRatioData = createMetdataFrame_annual()
    quartelyRatioData = createMetdataFrame_qtr() 
    columns = data.columns.tolist()
    for _, row in data.iterrows():
        qtr = ''
        for c in columns:
            metric_value = str(row[c])
            year = str(c).split("-")
            if (c == 'field_name'):
                metric_name = row[c]
                # print(metric_name)
            datestr = str(c).split()
            for i in datestr:
                if i != 'field_name':
                    dateObj = conStrToDateTime(i)
                    qtr = getQtrFromDt(dateObj)   

            metricDtlsrow = mtricsdf.loc[mtricsdf['metric_name'] == metric_name]
            if (metricDtlsrow.empty):
                continue
            m_id = metricDtlsrow['metric_id'].values[0]
            m_name = metricDtlsrow['metric_name'].values[0]
            m_level = metricDtlsrow['level'].values[0]
            m_parent = metricDtlsrow['parent'].values[0]
            # print(m_name)
            if frequency =='A':
                new_row = {'metric_id': str(m_id), 'metric_name':metric_name, 'metric_year':year[0], 'metric_value':metric_value
                                , 'company_code':ticker, 'parent_metric_id':str(m_parent),
                                'metric_level':str(m_level), 'created_by':'"user1"', 'updated_by':'"user1"'}
                annualRatioData.loc[len(annualRatioData.index)] = new_row
            elif frequency == 'Q':
                Qtr_row = {'metric_id': str(m_id), 'metric_name':metric_name, 'metric_year':year[0],'metric_quarter':qtr, 'metric_value':metric_value
                            , 'company_code':ticker, 'parent_metric_id':str(m_parent),
                            'metric_level':str(m_level), 'created_by':'"user1"', 'updated_by':'"user1"'}
                quartelyRatioData.loc[len(quartelyRatioData.index)] = Qtr_row
                
    if frequency == 'A':     
        filtered_df = annualRatioData[annualRatioData['metric_year'].str.contains('field_name') == False]
        print(filtered_df)
        filtered_df.to_excel("filtered_data_annual.xlsx")
        populateStaging('met_data_yearly_staging_tbl', filtered_df,frequency)
        merging_annual_data()
   
    elif frequency == 'Q':
        filtered_qtr_df = quartelyRatioData[quartelyRatioData['metric_year'].str.contains('field_name') == False]
        print(filtered_qtr_df)
        filtered_qtr_df.to_excel("filtered_data_qtr.xlsx")
        populateStaging('met_data_staging_tbl', filtered_qtr_df,frequency)
        merging_qtr_data()
        



def createMetdataFrame_annual():
    Edf = pd.DataFrame(columns=['metric_id','metric_name','metric_year','metric_value',
                                    'company_code','parent_metric_id','metric_level',"created_by","updated_by"])
    return Edf



def createMetdataFrame_qtr():
    Edf = pd.DataFrame(columns=['metric_id','metric_name','metric_year','metric_quarter','metric_value',
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



def getQuarter():
	# read in your SQL query results using pandas
	quarterdf = pd.read_sql("""
        SELECT quarter_code, quarter_name
        FROM dt_retail.quarter_tbl
        ORDER BY quarter_code
        """, engine)
	return quarterdf


qtrdf = getQuarter()
mtricsdf = getMetrics()



@profilehooks.timecall
def populateStaging(met_data_staging_tbl, edf,frequency):
    a = frequency
    truncateStagingTbl(a)
    edf.to_sql(met_data_staging_tbl, con=engine, schema='dt_retail', if_exists='append', index=False)
    print("---checking edf ---")
    # print(edf)


@profilehooks.timecall
def truncateStagingTbl(frequency):
    conn = engine.raw_connection()
    if frequency == 'A':
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
    elif frequency == 'Q':
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
def merging_annual_data():
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            print('in the iff for annual_ratio')
            cur.execute("""replace INTO key_ratios_yearly_tbl (metric_id, metric_name, metric_year, metric_value, 
                    company_code, parent_metric_id, metric_level, created_by, updated_by)
                    SELECT metric_id, metric_name, metric_year, metric_value, company_code, parent_metric_id, metric_level, created_by, updated_by
                    FROM met_data_yearly_staging_tbl;""")
    except:
        conn.rollback()
        raise Exception("Sorry, insert into key_ratios_annual_tbl failed. Rolling back")
    else:
        conn.commit()
    finally:
        conn.close()
            
            
def merging_qtr_data():
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            print('in the iff for qtr_ratio')
            cur.execute("""replace INTO key_ratios_quarterly_tbl (metric_id, metric_name, metric_year,metric_quarter, metric_value, 
                        company_code, parent_metric_id, metric_level, created_by, updated_by)
                        SELECT metric_id, metric_name, metric_year,metric_quarter, metric_value, company_code, parent_metric_id, metric_level, created_by, updated_by
                        FROM met_data_staging_tbl;""")
    except:
        conn.rollback()
        raise Exception("Sorry, insert into key_ratios_qtr_tbl failed. Rolling back")
    else:
        conn.commit()
    finally:
        conn.close()
                

def conStrToDateTime(datetime_str):
    try:
        datetime_object = datetime.strptime(datetime_str, '%Y-%m-%d')
        return datetime_object
        
    except ValueError as ve:    
        print('ValueError Raised:', ve)
        
        
def getQtrFromDt(datetimeObj):

    qtrOfDate = f'Q{(datetimeObj.month-1)//3+1}'
    return qtrOfDate
         

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
    # getData( "M", 'macys' , 'A')
        getData(ticker, company_name, 'A')
        # getData(ticker, company_name, 'Q')
        
        
main()
