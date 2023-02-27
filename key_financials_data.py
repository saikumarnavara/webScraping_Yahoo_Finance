import pandas as pd
import mysql.connector



ticker_input = input("Enter Company Code:- ")


#function to get data from key_financils 

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
    df1.to_excel("key_financials_filtered.xlsx")
    return df1

def get_keys_list():
    fh = open("key_financials.txt") 
    keys_string = fh.read()
    key_list = keys_string.split("\n")
    key_list = list(filter(None,key_list))
    return key_list


#function to get data from key_ratios_yearly 

def getYrlyRatioData(ticker):
    queries = ["select * from dt_retail.key_ratios_yearly_tbl;"]
    ratiodf = pd.DataFrame()
    for query in queries:
        sql_select_Query = query
        cursor = connection.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        yearly_data={}
        for row in records:  
            if row[5] == ticker:
                if(yearly_data.get("Key Ratios")==None):
                    yearly_data["Key Ratios"]=[row[2]]
                else:
                    if row[2] not in yearly_data["Key Ratios"]:
                        yearly_data["Key Ratios"].append(row[2])
                        
                if(yearly_data.get(row[3])==None):
                    yearly_data[row[3]]=[row[4]]
                else:
                    yearly_data[row[3]].append(row[4])
        # print(yearly_data)
        df = pd.DataFrame.from_dict(yearly_data)
        keys = get_keys_ratio_list()
        df = (df.loc[df['Key Ratios'].isin(keys)])
        ratiodf = pd.concat([df,ratiodf])
    print(ratiodf)
    ratiodf.to_excel("keyratios_filtered.xlsx")
    return ratiodf


#function to get data from key_ratios_quarterly 

def getQtrRatioData(ticker):
    queries = ["select * from dt_retail.key_ratios_quarterly_tbl;"]
    ratio_qtr_df = pd.DataFrame()
    for query in queries:
        sql_select_Query = query
        cursor = connection.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        yearly_data={}
        for row in records:  
            if row[6] == ticker:
                if(yearly_data.get("Key Ratios")==None):
                    yearly_data["Key Ratios"]=[row[2]]
                else:
                    if row[2] not in yearly_data["Key Ratios"]:
                        yearly_data["Key Ratios"].append(row[2])
                        
                if(yearly_data.get(row[3])==None):
                    yearly_data[row[3]]=[row[5]]
                else:
                    yearly_data[row[3]].append(row[5])
        # print(yearly_data)
    
        
        df1 = pd.DataFrame.from_dict(yearly_data,orient='index')
        df1 = df1.transpose()

        # print(df1)
        keys = get_keys_ratio_list()
        df1 = (df1.loc[df1['Key Ratios'].isin(keys)])
        ratio_qtr_df = pd.concat([df1,ratio_qtr_df])
    print(ratio_qtr_df)
    ratio_qtr_df.to_excel("keyratios_Yrly_filtered.xlsx")
    return ratio_qtr_df


def get_keys_ratio_list():
    fh = open("key_ratios.txt") 
    keys_string = fh.read()
    key_list = keys_string.split("\n")
    key_list = list(filter(None,key_list))
    return key_list

def main():
    get_Data_from_db(ticker_input)
    getYrlyRatioData(ticker_input)
    getQtrRatioData(ticker_input)
    print("---completed---")
    
main()

