import pandas as pd
import mysql.connector

connection = mysql.connector.connect(
    host = "uaa-db.mysql.database.azure.com",
    user = "wadmin@uaa-db",
    password = ("Tcs#1234") 
    )

ticker_input = input("Enter Company Code:- ")


def get_Data_from_db(ticker):
    queries = ["select * from dt_retail.income_statement_yearly_tbl;",
               "select * from dt_retail.balance_sheet_yearly_tbl;",
               "select * from dt_retail.cash_flow_yearly_tbl;"]
    df1 = pd.DataFrame()
    for query in queries:
        sql_select_Query = query
        cursor = connection.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        
        yearly_data={}
        for row in records:  
            if row[5] == ticker:
                if(yearly_data.get("Key Financials")==None):
                    yearly_data["Key Financials"]=[row[2]]
                else:
                    if row[2] not in yearly_data["Key Financials"]:
                        yearly_data["Key Financials"].append(row[2])
                        
                if(yearly_data.get(row[3])==None):
                    yearly_data[row[3]]=[row[4]]
                else:
                    yearly_data[row[3]].append(row[4])
                    
        data = pd.DataFrame.from_dict(yearly_data)
        keys = get_keys_list()
        data = (data.loc[data['Key Financials'].isin(keys)])
        df1 = pd.concat([data,df1])
    print(df1)
    print(type(df1))
    df1.to_excel("key_financials.xlsx")
    return df1

def get_keys_list():
    fh = open("key_financials.txt") 
    keys_string = fh.read()
    key_list = keys_string.split("\n")
    key_list = list(filter(None,key_list))
    return key_list

def main():
    get_Data_from_db(ticker_input)
    print("---completed---")
    
main()

