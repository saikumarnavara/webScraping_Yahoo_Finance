# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# import pandas as pd

# chrome_path = r"C:\Users\Jawahar\Desktop\chromedriver_win32\chromedriver.exe"
# s = Service(chrome_path)
# driver = webdriver.Chrome(service=s)

# driver.get('https://finance.yahoo.com/quote/TSLA/financials?p=TSLA')

# WebDriverWait(driver,30).until(EC.visibility_of_element_located((By.XPATH, '//div[@class="D(tbhg)"]')))

# headers_elem = driver.find_elements(By.XPATH, '//div[@class="D(tbhg)"]/div/div')
# col_headers = [header.text for header in headers_elem]

# df = pd.DataFrame(columns = col_headers)


# rows = driver.find_elements(By.XPATH, '//div[@class="D(tbrg)"]//div[@data-test="fin-row"]')
# data  = []
# for row in rows:
#     row_values = row.find_elements(By.XPATH, '//div[@class="D(tbrg)"]')
#     data.append(row_values)
#     # a = [row_value.text for row_value in row_values]
# #s = []
# #for i in range(1):
# j = data[0]
# s=[i for i in j[0].text.split("\n")]
    

# #dict_a = {}
# #for i in range(len(s)):
# #    if i % 2 != 0:
# #        dict_a[s[i-1]]=s[i]
# #k=[]
# for i in range(len(s)):
#     if(i%2!=0):
#         k=[]
#         k.append(s[i-1])
#         for l1 in s[i].split(" "):
#             k.append(l1)
#         df.loc[len(df)] = k
   
# print(df)


# df.to_excel(r'C:\Users\Jawahar\Desktop\web scrap\Yahoofinance.xlsx', index=False)
# df.to_csv(r'C:\Users\Jawahar\Desktop\web scrap\Yahoofinance.csv', index=False)






# # rows = driver.find_elements(By.XPATH, '//div[@class="D(tbrg)"]//div[@data-test="fin-row"]')
# # row_values = [row_value.text for row_value in rows]

# #df1 = pd.DataFrame(row_values)
# #print (df1)

# #df1.to_excel(r'C:\Users\Jawahar\Desktop\web scrap\Yahoofinance.xlsx', index=False)
# #df1.to_csv(r'C:\Users\Jawahar\Desktop\web scrap\Yahoofinance.csv', index=False)

import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
import json

hdr = {'User-Agent':'Mozilla/5.0'}
url = 'https://www.macrotrends.net/stocks/charts/TSLA/tesla/financial-ratios'

response = requests.get(url, headers=hdr)
soup = bs(response.content, 'html.parser')


data = soup.find_all('script') #the relevant data is inside one of many <script> tags
for dat in data:
    if 'Operatin' in dat.text: #this locates the specific script tag containing the data

        #the next 3 lines remove the parts before and after the relevant data in the relevant script, and isolate the target info
        one = dat.text.split('var originalData = ') 
        two = one[1].split('var source =')
        candidate = two[0].strip().replace('];',']')

        data = json.loads(candidate) #having isolated the data - which is is json format, we assign it to a variable
        first_df= pd.read_json(candidate).drop(columns='popup_icon') #the json is loaded into a pandas datafram, and irrelevant stuff is dropped

#the right/first column in the data is now extracted out of the html code in which it resides and converted to a list

annual_data = []
series_df = first_df.field_name.dropna()
for i in series_df:
    annual_data.append(bs(i,'lxml').text)

first_df['Annual Data'] = pd.Series(annual_data) #the list is converted to a pandas Series and added to the dataframe as a new column
final_df=first_df.drop(columns='field_name').set_index('Annual Data')#the original column with the html code is dropped 
final_df.to_excel(r'tesla.xlsx') #finally, the dataframe is writtedn to a csv file