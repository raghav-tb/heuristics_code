# Imports
import psycopg2
import pandas as pd
import sys
import enchant
import numpy as np


# Database Credentials:
hostname = '34.122.223.157'
database = 'lighthouse'
username = 'lighthouse_user'
password = 'qNdZ8ru5beoJO2DY'
port_id = '5432'



def get_isin_symbol_data():
    try:
        conn = None
        curr = None
        conn = psycopg2.connect(
                host = hostname,
                dbname = database,
                user = username,
                password = password,
                port = port_id,
            )

        curr = conn.cursor()
        curr.execute('select * from isincode_symbolcode_mapping ism')
        ans = curr.fetchall()
        conn.commit()
        return ans
    except Exception as e:
        print("DBError:",e)
    finally:
        if curr is not None:
            curr.close()
        if conn is not None:
            conn.close()


def df_format(df):
	df.replace(np.nan, '', regex=True)
	
	# Format Column Names
	column_rename_mapper = {
		'ASONDATE':'report_date',
		'ISINCODE':'unique_id',
		'QUANTITY':'quantity',
		'UNITCOST':'average_unit_cost',
		'MARKETRATE':'market_rate',
		'SYMBOLNAME':'symbol_name',
		'NAME':'fund_id',
		'ASTCLS':'instrument_type'
	}
	df.rename(columns = column_rename_mapper, inplace = True)
	
	# Rename Fund IDs
	scheme_rename_mapper = {
		"TRUE BEACON AIF-TRUE BEACON AIF SCHEME 1  ": "TB1",
		"TRUE BEACON AIF-TRUE BEACON AIF SCHEME 2  ": "TB2",
		"TRUE BEACON AIF-TRUE BEACON AIF SCHEME GLOBAL  ": "TBG", 
		"TBGAS1":"TBG"
	}
	df["fund_id"].replace(scheme_rename_mapper, inplace=True)

	# Select only required columns
	required_cols = [
		'report_date', 
		'unique_id', 
		'quantity', 
		'average_unit_cost', 
		'market_rate', 
		'symbol_name', 
		'fund_id', 
		'instrument_type'
	]
	df = df[required_cols]

	# TODO: Remove filter for FUTURES only
	df = df[df['instrument_type'].isin(['FUTURES'])]
	
	df['report_date'] = pd.to_datetime(df.report_date, format="%d/%m/%Y")
	# df['report_date'] = df["report_date"].datetime.strftime("%d/%m/%Y")

	return df







def leven_score(str1,str2):
	res = enchant.utils.levenshtein(str1.upper(), str2.upper())
	return res



def main():

    # Database isin_code dataframe
    isin_res = get_isin_symbol_data()
    isin_df = pd.DataFrame(isin_res)
    isin_df.rename(columns = {0:'symbol_name',1:'symbol_code',2:'isin_code'}, inplace=True)
    # print("Before Symbol_code:",isin_df.head(2))
    print("Isin table no. of rows:",len(isin_df.index))

    # Input file dataframe
    input_raw = pd.read_csv('./2022-06-17_Portfolio Appraisal Statement Fund level.csv')
    input_df = df_format(input_raw)
    # print(input_df.head(2))
    print("Input Dataframe rows:",len(input_df.index))
    

    # Calculate Leven Score & Store the results:
    for index,column in input_df.iterrows():
        min_leven = sys.maxsize
        store_index = -1
        store_symbol_code = ''
        for i,col in isin_df.iterrows():
            live_score = leven_score(column['symbol_name'][:-13],col['symbol_name'])
            # print(column['symbol_name'][:-13],col['symbol_name'])
            live_score -= abs(len(column['symbol_name'][:-13]) - len(col['symbol_name']))
            if(live_score < min_leven):
                min_leven = live_score
                store_index = index
                store_symbol_code = col['symbol_code']
        if store_index == -1 or store_symbol_code == '':
            print("No match for this row.Ingestion Stopped.")
            break
        input_df.loc[index,'symbol_code'] = store_symbol_code

    # print("SymbolCode added:",input_df.head(2))
    input_df.to_csv("dbname_to_name.csv")
    
    

    

main()