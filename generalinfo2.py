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



# connection = mysql.connector.connect(
#     host = "uaa-db.mysql.database.azure.com",
#     user = "wadmin@uaa-db",
#     password = ("Tcs#1234") 
#     )

# engine = create_engine("mysql://%s:Tcs#1234@uaa-db.mysql.database.azure.com:3306" % quote_plus("wadmin@uaa-db"))

engine = create_engine("mysql://%s:Tcs#1234@uaa-db-migrated.mysql.database.azure.com:3306/dt_retail" % quote_plus("wadmin"))



# Set the URL for the company profile page
def getGeneralInfo():
    generalInfoData= generalInfoDF()
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
        
        description_element = soup.find("p", class_="Mt(15px) Lh(1.6)")
        description = description_element.text

        companyName = soup.find('h3',class_="Fz(m) Mb(10px)")
        company_name = companyName.text

        general_info2= soup.find("div", class_="Mb(25px)")
        coloumn1 = general_info2.find("p" , class_='D(ib) W(47.727%) Pend(40px)')
        coloumn2 = general_info2.find("p" , class_="D(ib) Va(t)")
        
        html_col1 = coloumn1.prettify()
        coloumnData = get_text(html_col1)
        
        coloumn_one_data = coloumnData.splitlines()
        # print(coloumn_one_data)
        website = coloumn_one_data[-1]
        # print(website)
        headQuarter = coloumn_one_data[2]
        # print(headQuarter)
        
        html_col2 = coloumn2.prettify()
        text_col2 = get_text(html_col2)
        text = text_col2.split("\n")
        text_coloumn2 = list(filter(None, text))
        
        
        total_emp = text_coloumn2[-1]
        total_emp = (total_emp.split(":")[1])
        Employees = total_emp.replace(',','')
        
        segment = text_coloumn2[-2]
        Industry_segment = (segment.split(":")[1]).lstrip()
        companyCode = ticker
        
        sector_value = text_coloumn2[-3]
        sector = (sector_value.split(":")[1]).lstrip()
        # print(sector)
        # print(Industry_segment)
        
        
        
        
        #statistics logic from here
        url = (f"https://finance.yahoo.com/quote/{ticker}/key-statistics?p={ticker}")
        driver = webdriver.Chrome()
        driver.get(url)
        driver.implicitly_wait(10)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        all_tables = [[
        [td.get_text(strip=True) for td in tr.find_all('td')] 
        for tr in table.find_all('tr')] 
        for table in soup.find_all('table')]

        
        flat_list = []
        for sublist in all_tables:
            for item in sublist:
                flat_list.append(item)
        # print("in staticticscode")           
        new =[]
        data = ['52 Week High3','52 Week Low3','Shares Outstanding5']
        for i in flat_list:
            for j in data:
                if i[0] == j:
                    new.append(i)
        high_52_week = new[0][1]
        low_52_week = new[1][1]
        share_outstanding = new[2][1]
        s_o = re.sub('([A-Z])', r' \1', str(share_outstanding)) #space before capital letter
        # l = [share_outstanding]
        # m = {'M': 6, 'B' : 9, 'T' : 12,}
        # share_outstanding=([int(float(i[:-1]) * 10 ** m[i[-1]]) for i in l])
        # print(high_52_week)
        # print(low_52_week)
        # print(s_o)
    
    # ####sumarry code
        url = (f"https://finance.yahoo.com/quote/{ticker}?p={ticker}")
        driver = webdriver.Chrome()
        driver.get(url)
        driver.implicitly_wait(10)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        # soup.select("table > tr:has(> tr > a:contains('Volume'))td")
        all_tables = [[
        [td.get_text(strip=True) for td in tr.find_all('td')] 
        for tr in table.find_all('tr')] 
        for table in soup.find_all('table')]
        table_1 = all_tables[0]
        table_2 = all_tables[1]
        data_1 = ['Volume', 'Avg. Volume']
        data_2 =['Market Cap', 'PE Ratio (TTM)']
        
        list_1 = []
        for i in table_1:
            for j in data_1:
                if i[0] == j:
                    list_1.append(i)
        list_2 = []
        for i in table_2:
            for j in data_2:
                if i[0] == j:
                    list_2.append(i)
                    
        
        volume = list_1[0][1].replace(',','')
        average_volume = list_1[1][1].replace(',','')
        Market_cap = list_2[0][1]
        pe_ratio = list_2[1][1]
        m_c = re.sub('([A-Z])', r' \1', str(Market_cap)) #space before capital letter
        # l = [Market_cap]
        # print(l)
        # m = {'B': 9,'M' : 6, 'T' : 12,} 
        # Market_cap=([int(float(i[:-1]) * 10 ** m[i[-1]]) for i in l])
        # print(volume)
        # print(average_volume)
        # print(m_c)
        # print(pe_ratio)

    
        gnrlInfo_row = {'company_code': companyCode,'company_name': company_name,'description': description,'head_quarter': headQuarter, 'sector':sector,'website':website,
        'employees': Employees,'industry_segment': Industry_segment,'shares_outstanding': s_o,'volume': volume,'volume_average': average_volume,
        'high_52_week': high_52_week,'low_52_week': low_52_week,'p_by_e': pe_ratio,'marketcap': m_c }
        generalInfoData.loc[len(generalInfoData.index)] = gnrlInfo_row
        generalInfoData.to_excel("gnrlInfo2.xlsx")
    populate_gnrlInfo_Staging('general_info_staging_tbl2',generalInfoData)
    merge_Gnrl_Info_Intotbl()




def generalInfoDF():
    Edf = pd.DataFrame(columns=['company_code','company_name','description','head_quarter','sector','website','employees','industry_segment','shares_outstanding','volume','volume_average','high_52_week','low_52_week','p_by_e','marketcap'])
    return Edf

    
# def get_company_codes():
#     fh = open("tickers.txt") 
#     tickers_string = fh.read()
#     tickers_list = tickers_string.split("\n")
#     tickers_list = list(filter(None,tickers_list))
#     return tickers_list

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

def populate_gnrlInfo_Staging(general_info_staging_tbl2, edf):
    print("in populating")
    truncate_GnrlInfo_StagingTbl()
    edf.to_sql(general_info_staging_tbl2, con=engine, schema='dt_retail', if_exists='append', index=False)

def truncate_GnrlInfo_StagingTbl():
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""truncate table dt_retail.general_info_staging_tbl2;""")
            print("table truncated")
    except:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()

def merge_Gnrl_Info_Intotbl():
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            print('in the iff for general_info')
            cur.execute("""replace INTO dt_retail.general_info_tbl2 (company_code,company_name,description,head_quarter,sector,website,employees,industry_segment,shares_outstanding,volume,volume_average,high_52_week,low_52_week,p_by_e,marketcap)
                SELECT company_code,company_name,description,head_quarter,sector,website,employees,industry_segment,shares_outstanding,volume,volume_average,high_52_week,low_52_week,p_by_e,marketcap
                FROM dt_retail.general_info_staging_tbl2;""")
    except:
        conn.rollback()
        raise Exception("Sorry, insert into general_info_tbl2 failed. Rolling back")
    else:
        conn.commit()
    finally:
        conn.close()      

     
def main():
    getGeneralInfo()

main()

