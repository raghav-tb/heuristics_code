#Making all necessary imports for the code
from jugaad_data.nse import bhavcopy_fo_save
import pandas as pd
from jugaad_data.holidays import holidays
from random import randint
import time, os
import numpy as np
import datetime
import enchant
import psycopg2
import psycopg2.extras
import sys

# Database Credentials:
hostname = '34.122.223.157'
database = 'lighthouse'
username = 'lighthouse_user'
password = 'qNdZ8ru5beoJO2DY'
port_id = '5432'

date_range = pd.bdate_range(start='07/08/2022',end='07/08/2022', 
						 freq='C', holidays = holidays(2022,12))
						 
savepath = os.path.join('D:', os.sep, 'True_Beacon\heuristics_code')
												  
# start and end dates in "MM-DD-YYYY" format
# holidays() function in (year,month) format
#freq = 'C' is for custom

dates_list = [x.date() for x in date_range]

for dates in dates_list:
	try:
		bhavcopy_fo_save(dates, savepath)
		time.sleep(randint(1,4)) #adding random delay of 1-4 seconds
	except (ConnectionError, TimeoutError) as e:
		time.sleep(10) #stop program for 10 seconds and try again.
		try:
			bhavcopy_fo_save(dates, savepath)
			time.sleep(randint(1,4))
		except (ConnectionError, TimeoutError) as e:
			print(f'{dates}: File not Found')

# Format Input Dataframe
def input_df_format():
	df = pd.read_csv('./2022-07-08_Portfolio Appraisal Statement Fund level.csv')
	df.replace(np.nan, '', regex=True)
	
	# Format Column Names
	column_rename_mapper = {
		'ASONDATE':'report_date',
		'ISINCODE':'unique_id',
		'QUANTITY':'quantity',
		'UNITCOST':'average_unit_cost',
		'MARKETRATE':'market_rate',
		'SYMBOLNAME':'symbol_name',
		'SYMBOLCODE':'symbol_code_input',
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
		'symbol_code_input',
		'fund_id', 
		'instrument_type'
	]
	df = df[required_cols]

	# TODO: Remove filter for FUTURES only
	df = df[df['instrument_type'].isin(['OPTIONS'])]
	
	df['report_date'] = pd.to_datetime(df.report_date, format="%d/%m/%Y")
	# df['report_date'] = df["report_date"].datetime.strftime("%d/%m/%Y")

	return df


# Format bhavcopy dataframe
def bhav_copy_df_format():
	df = pd.read_csv('./fo08Jul2022bhav.csv')
	column_rename_mapper = {
		# 'OPEN':'open_bhav',
		# 'HIGH':'high_bhav',
		# 'LOW':'low_bhav',
		# 'SETTLE_PR':'settle_price_bhav',
		# 'CONTRACTS':'contracts_bhav',
		# 'VAL_INLAKH':'value_in_lakhs_bhav',
		# 'OPEN_INT':'open_interest_bhav',
		# 'CHG_IN_OI':'change_in_oi_bhav',
		# 'TIMESTAMP':'timestamp_bhav',
		'CLOSE':'market_rate',
		'SYMBOL':'symbol_code',
		'OPTION_TYP':'option_type_bhav',
		'EXPIRY_DT':'expiry_date',
		'INSTRUMENT':'instrument_type_bhav'
	}
	df.rename(columns = column_rename_mapper, inplace = True)
	required_cols = [
		# 'open_bhav','high_bhav','low_bhav','settle_price_bhav','contracts_bhav',
		# 'value_in_lakhs_bhav','open_interest_bhav','change_in_oi_bhav','timestamp_bhav','option_type_bhav',
		'market_rate','symbol_code','expiry_date','instrument_type_bhav'
	]
	df = df[required_cols]

	# Rename Instrument_type
	df = df[df['instrument_type_bhav'].isin(['OPTIDX','OPTSTK'])]
	scheme_rename_mapper = {
		"OPTIDX": "OPTIONS",
		"OPTSTK": "OPTIONS"
	}
	df["instrument_type_bhav"].replace(scheme_rename_mapper, inplace=True)
	# df['expiry_date'] = pd.to_datetime(df["expiry_date"].at.strftime('%d-%b-%Y'))
	df['expiry_date'] = pd.to_datetime(df.expiry_date, format="%d-%b-%Y").dt.strftime('%d/%m/%Y')
	return df


# Format expirydate
def expirydate(str):
	temp = datetime.datetime.strptime(str[-10:],'%d/%m/%Y').strftime('%d-%b-%Y')
	return temp

# Calculate levenshtein value for two strings:
def leven_score(str1,str2):
	res = enchant.utils.levenshtein(str1.upper(), str2.upper())
	return res

# Get isin_symbolcode_mapping data 
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

# Get future_nse_symbolcode_mapping data:
def get_future_nse_symbolcode_data():
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
		curr.execute('select * from future_nse_symbolcode_mapping fnsm')
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

# Calculate unique id:
def calculate_unique_id(df):
	df['unique_id'] = df['symbol_code'] + pd.to_datetime(df['symbol_name'].str.split(' ').str[-3], format="%d/%m/%Y").dt.strftime('%y%b').str.upper() + df['symbol_name'].str.split(' ').str[-1] + df['symbol_name'].str.split(' ').str[-5].str[0] + 'E'
	# print(df['symbol_code'] + pd.to_datetime(df['symbol_name'].str.split(' ').str[-3], format="%d/%m/%Y").dt.strftime('%y%b').str.upper() + df['symbol_name'].str.split(' ').str[-1] + df['symbol_name'].str.split(' ').str[-5].str[0])
	return df

# Format data for future_nse_symbolcode table:
def hdfc_nse_format(df):
	df = df[['symbol_code_input','unique_id']]
	df.columns = ['hdfc_symbol_code','nse_symbol_code']
	# print(df.head(1))
	return df

# Push Data to custodian_holdings table:
def custodian_holding_dbpush(df,typerow):
	df = df[['report_date','unique_id','quantity','average_unit_cost','market_rate','symbol_name','instrument_type','fund_id']]
	try:
		conn = psycopg2.connect(
				host = hostname,
				dbname = database,
				user = username,
				password = password,
				port = port_id,
			)

		curr = conn.cursor()
		count = 0
		inser_script = 'INSERT INTO custodian_holdings (report_date, unique_id, quantity, average_unit_cost, market_rate, symbol_name, instrument_type, fund_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'
		for index,row in df.iterrows():
			insert_value = (row['report_date'],row['unique_id'],row['quantity'],row['average_unit_cost'],row['market_rate'],row['symbol_name'],row['instrument_type'],row['fund_id'])
			curr.execute(inser_script,insert_value)
			# print("Insert Values:",insert_value)
			count += 1
		print("Number of Entries of " + typerow + ' is: ' + str(count))
		# send_to_slack("Number of Entries added to DB for fund_id " + fund_id + ' is: ' + str(count))
		conn.commit()
	except Exception as e:
		print("DBError:",e)
	finally:
		if curr is not None:
			curr.close()
		if conn is not None:
			conn.close()

# Push Data to future_nse_symbolcode table:
def hdfc_nse_symbolcode_dbpush(dataframe,typerow):
	connection = None
	curr = None
	try:
		connection = psycopg2.connect(
				host = hostname,
				dbname = database,
				user = username,
				password = password,
				port = port_id,
			)

		cursor = connection.cursor()
		records = dataframe.to_dict(orient='records')
		columns = records[0].keys()
		query = 'INSERT INTO future_nse_symbolcode_mapping ({}) VALUES %s'.format(','.join(columns))
		values = [[value for value in record.values()] for record in records]
		psycopg2.extras.execute_values(cursor, query, values)
		print(typerow + 'Data Pushed to nse_symbolcode_mapping.')
		connection.commit()
	except Exception as e:
		print("DBError:",e)
	finally:
		if curr is not None:
			curr.close()
		if connection is not None:
			connection.close()


# EntryPoint:
def main():
# Formated df of TB1
	input_df = input_df_format()
	print("Input dataframe length:",len(input_df.index))
	# print(input_df)
	# print("TB Dataframe:",input_df.head(2))
	# input_df.to_csv("input_df.csv")
	

# Formated df of FUTURES for that date:
	bhav_copy_df = bhav_copy_df_format()
	# bhav_copy_df.to_csv("bhav.csv")
	# print("BhavCopy: ",bhav_copy_df.head(2))
	print("bhavcopy length:",len(bhav_copy_df.index))

# ISIN table 
	isin_res = get_isin_symbol_data()
	isin_df = pd.DataFrame(isin_res)
	isin_df.rename(columns = {0:'symbol_name',1:'symbol_code',2:'isin_code'}, inplace=True)

# hdfc and nse table mapping:
	fut_nse_res = get_future_nse_symbolcode_data()
	fut_nse_df = pd.DataFrame(fut_nse_res)
	fut_nse_df.rename(columns = {0:'hdfc_symbol_code',1:'nse_symbol_code'}, inplace=True)
	print("Future NSE dataframe length:",len(fut_nse_df.index))
	# print("Future Dataframe:",fut_nse_df)

	

# symbol_code is already present or not into DB:
	new_input_df = pd.DataFrame()
	already_present_df = pd.DataFrame()

	for index,column in input_df.iterrows():
		check = 0
		if len(fut_nse_df.index) != 0:
			check = (column['symbol_code_input'] == fut_nse_df['hdfc_symbol_code']).sum()
		# print(check)
		if check == 1:
			raw_df = column.to_frame().T
			already_present_df = pd.concat([already_present_df,raw_df], ignore_index = True)
		else:
			raw_df = column.to_frame().T
			new_input_df = pd.concat([new_input_df,raw_df], ignore_index = True)

		

	print("already present length:",len(already_present_df.index))
	print("new input datframe:",len(new_input_df.index))
	
	# return

# Unique Id calculation for already present match:
	if len(fut_nse_df.index) != 0 & len(already_present_df.index) != 0:
		already_present_df = pd.merge(already_present_df,fut_nse_df,left_on='symbol_code_input',right_on='hdfc_symbol_code')
		already_present_df['unique_id'] = already_present_df['nse_symbol_code'] + pd.to_datetime(already_present_df.symbol_name.str[-10:], format="%d/%m/%Y").dt.strftime('%y%b').str.upper() + 'FUT'
		already_present_df.drop(['nse_symbol_code','hdfc_symbol_code','symbol_code_input'],axis=1,inplace=True)
		print("Length of already present:",len(already_present_df.index))



# Input dataframe divides into total 3 part: 
	# 1. Zero Match: Drop them from input dataframe
	# 2. Exact Match & Conflicted Match: Add both of them into different dataframe.

	no_of_zero_match = 0
	no_of_conflicted_match = 0
	exactmatch_df = pd.DataFrame()
	conflicted_df = pd.DataFrame()
	for index,column in new_input_df.iterrows():
		count = (column['market_rate'] == bhav_copy_df['market_rate']).sum()
		if count == 0:
			no_of_zero_match += 1
			new_input_df = new_input_df.drop(index)
		elif count == 1:
			# print("Type Check:",type(column))
			raw_df = column.to_frame().T
			exactmatch_df = pd.concat([exactmatch_df,raw_df], ignore_index = True)
			# print(exactmatch_df)
		else:
			no_of_conflicted_match += 1
			con_raw_df = column.to_frame().T
			conflicted_df = pd.concat([conflicted_df,con_raw_df], ignore_index = True)
	
	print("exact match length:",len(exactmatch_df.index))
	print("conflicted length:",len(conflicted_df.index))
	print("zero match length:",no_of_zero_match)

	# return

# Exact match merge with bhav_copy and get symbol_code then calculate unique_id:
	if len(exactmatch_df.index) != 0:
		em_merge_df = pd.merge(exactmatch_df,bhav_copy_df,on='market_rate')
		print("exact match length:",len(em_merge_df.index))
		em_merge_df.drop(['instrument_type_bhav'],axis=1,inplace=True)
		em_merge_df = calculate_unique_id(em_merge_df)
		# em_merge_df.to_csv("exactmatch.csv")
	else:
		em_merge_df = pd.DataFrame()

	
	# print(exactmatch_df)
	# return 
	# print("Two Head:",em_merge_df.head(2))

	
# Conflicted row start checking by both ways: 
# 	1. bhav_copy_check using fields: instrument_type,expiry_date and close_price.
#	2. isin_symbolcode_mapping table name matching and get best match.

	for index,column in conflicted_df.iterrows():
		# Variables:
		min_leven_first = sys.maxsize
		min_leven_second = sys.maxsize
		store_code_first = ''
		store_code_second = ''

		# First Apporach best match: (Name Vs Code match)
		for i,col in bhav_copy_df.iterrows():
			if column['market_rate'] == col['market_rate']:
				live_score = leven_score(column['symbol_name'][:-28],col['symbol_code'])
				# print("first apporach",column['symbol_name'][:-27],col['symbol_code'])
				live_score -= abs(len(column['symbol_name'][:-28]) - len(col['symbol_code']))
				if(live_score < min_leven_first):
					min_leven_first = live_score
					store_code_first = col['symbol_code']
		if store_code_first == '':
			print("No match found for index: " + str(index) + " into BhavCopy file.")
			break

		# Second Apporach best match: (Name Vs DB Name)
		for i,col in isin_df.iterrows():
			live_score = leven_score(column['symbol_name'][:-28],col['symbol_name'])
			# print(column['symbol_name'][:-28],col['symbol_name'])
			live_score -= abs(len(column['symbol_name'][:-28]) - len(col['symbol_name']))
			# print(live_score)
			if(live_score < min_leven_second):
				min_leven_second = live_score
				store_code_second = col['symbol_code']
		if store_code_second == '':
			print("No match found for index: " + str(index) + " into isin_code(master table).")
			break

		print("Matched Check: ",store_code_first + ' & '  + store_code_second)
		if(store_code_first == store_code_second):
			conflicted_df.loc[index,'symbol_code'] = store_code_first
		else:
			print("Stopping Data the Ingestion.")
			return


	
	# print("ConflictedDataframe:",conflicted_df)
	# return
	# print("length of exact match:",len(exactmatch_df.index))
	# print("conflicted Dataframe length:",len(conflicted_df.index))
	# conflicted_df.to_csv("beforeconflicted.csv")


# Conflicted dataframe merge with bhav_copy and calculate unique_id:
	if len(conflicted_df.index) != 0:
		conflicted_df = pd.merge(conflicted_df,bhav_copy_df,on=['symbol_code','market_rate'])
		conflicted_df = conflicted_df[ (conflicted_df['expiry_date'] == conflicted_df['symbol_name'].str.split(' ').str[-3]) ]
		conflicted_df = conflicted_df.drop_duplicates()
		conflicted_df.drop(['instrument_type_bhav'],axis=1,inplace=True)
		conflicted_df = calculate_unique_id(conflicted_df)

	# print(conflicted_df)
	# return

# Custodian holding data push:
	if len(conflicted_df.index) != 0:
		custodian_holding_dbpush(conflicted_df,'no_of_conflicted_matchs')
	if len(em_merge_df.index) != 0:
		custodian_holding_dbpush(em_merge_df,'Exact Match rows')
	
# futures_nse_symbol_mapping data push:
	if len(conflicted_df.index) != 0:
		conflicted_symbol_push = hdfc_nse_format(conflicted_df)
		hdfc_nse_symbolcode_dbpush(conflicted_symbol_push,'no_of_conflicted_matchs')

	if len(em_merge_df.index) != 0:
		em_symbol_push = hdfc_nse_format(em_merge_df)
		hdfc_nse_symbolcode_dbpush(em_symbol_push,'Exact Match rows')


	
main()