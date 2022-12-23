# For data manipulation
import pandas as pd
from urllib.request import urlopen, Request
# To extract fundamental data
from bs4 import BeautifulSoup
import mysql.connector


connection = mysql.connector.connect(
    host = "uaa-db.mysql.database.azure.com",
    user = "wadmin@uaa-db",
    password = ("Tcs#1234") 
    )


def get_company_codes():
    sql_select_Query = "SELECT * FROM dt_retail.company_tbl;"
    cursor = connection.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    company_codes = []
    for row in records:
        codes  = (row[0])
        company_codes.append(codes)
    return company_codes

def fundamental_metric(soup, metric):
    return soup.find(text = metric).find_next(class_='snapshot-td2').text

def get_fundamental_data(df):
    for symbol in df.index:
        try:
            url = ("http://finviz.com/quote.ashx?t=" + symbol.lower())
            req = Request(url=url,headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'}) 
            response = urlopen(req)
            soup = BeautifulSoup(response,'html.parser')
            for m in df.columns:
                df.loc[symbol,m] = fundamental_metric(soup,m)
                                
        except Exception as e:
            print (symbol, 'not found')
    return df


# stock_list = ['GPC','AAP','KSS','MAKSY','M','HMRZF']  

metric = ['P/B',
'P/E',
'Forward P/E',
'PEG',
'Debt/Eq',
'EPS (ttm)',
'Dividend %',
'ROE',
'ROI',
'EPS Q/Q',
'Insider Own'
]

company_codes = get_company_codes()
df = pd.DataFrame(index=company_codes,columns=metric)
df = get_fundamental_data(df)



df['Dividend %'] = df['Dividend %'].str.replace('%', '')
df['ROE'] = df['ROE'].str.replace('%', '')
df['ROI'] = df['ROI'].str.replace('%', '')
df['EPS Q/Q'] = df['EPS Q/Q'].str.replace('%', '')
df['Insider Own'] = df['Insider Own'].str.replace('%', '')
df = df.apply(pd.to_numeric, errors='coerce')
print(df)
# print(type(df))

# for index, row in df.itercolumns():
#     metric_id = row[0]
#     print(metric_id)
  

    # print(str(metric_id) +" " + str(metric_value) + " " + str(value))


