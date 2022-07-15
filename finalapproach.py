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
import sys

# Database Credentials:
hostname = '34.122.223.157'
database = 'lighthouse'
username = 'lighthouse_user'
password = 'qNdZ8ru5beoJO2DY'
port_id = '5432'

date_range = pd.bdate_range(start='06/17/2022',end='06/17/2022', 
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


def input_df_format():
	df = pd.read_csv('./2022-06-17_Portfolio Appraisal Statement Fund level.csv')
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


def bhav_copy_df_format():
	df = pd.read_csv('./fo17Jun2022bhav.csv')
	column_rename_mapper = {
		'CLOSE':'market_rate',
		'SYMBOL':'symbol_code',
		'EXPIRY_DT':'expiry_date',
		'INSTRUMENT':'instrument_type'
	}
	df.rename(columns = column_rename_mapper, inplace = True)
	required_cols = [
		'market_rate', 
		'symbol_code', 
		'expiry_date', 
		'instrument_type', 
	]
	df = df[required_cols]

	# Rename Instrument_type
	df = df[df['instrument_type'].isin(['FUTIDX','FUTSTK'])]
	scheme_rename_mapper = {
		"FUTIDX": "FUTURES",
		'FUTSTK': "FUTURES"
	}
	df["instrument_type"].replace(scheme_rename_mapper, inplace=True)
	# df['expiry_date'] = pd.to_datetime(df["expiry_date"].at.strftime('%d-%b-%Y'))
	df['expiry_date'] = pd.to_datetime(df.expiry_date, format="%d-%b-%Y").dt.strftime('%d/%m/%Y')
	return df


def expirydate(str):
	temp = datetime.datetime.strptime(str[-10:],'%d/%m/%Y').strftime('%d-%b-%Y')
	return temp


def leven_score(str1,str2):
	res = enchant.utils.levenshtein(str1.upper(), str2.upper())
	return res





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




def main():
	# Formated df of TB1
	input_df = input_df_format()
	print("Input dataframe length:",len(input_df.index))
	# print("TB Dataframe:",input_df.head(2))
	input_df.to_csv("input_df.csv")


	#Formated df of FUTURES for that date:
	bhav_copy_df = bhav_copy_df_format()
	# bhav_copy_df.to_csv("bhav.csv")
	# print("BhavCopy: ",bhav_copy_df.head(2))


	isin_res = get_isin_symbol_data()
	isin_df = pd.DataFrame(isin_res)
	isin_df.rename(columns = {0:'symbol_name',1:'symbol_code',2:'isin_code'}, inplace=True)

	zero_match = 0
	conflicted_row = 0
	exactmatch_df = pd.DataFrame()
	conflicted_df = pd.DataFrame()
	for index,column in input_df.iterrows():
		count = (column['market_rate'] == bhav_copy_df['market_rate']).sum()
		if count == 0:
			zero_match += 1
			input_df = input_df.drop(index)
		elif count == 1:
			# print("Type Check:",type(column))
			raw_df = column.to_frame().T
			exactmatch_df = pd.concat([exactmatch_df,raw_df], ignore_index = True)
			# print(exactmatch_df)
		else:
			conflicted_row += 1
			con_raw_df = column.to_frame().T
			conflicted_df = pd.concat([conflicted_df,con_raw_df], ignore_index = True)

	em_merge_df = pd.merge(exactmatch_df,bhav_copy_df,on='market_rate')
	print("exact match length:",len(em_merge_df.index))
	# print("Two Head:",em_merge_df.head(2))

	# print("Conflicted df length:",len(conflicted_df.index))
	# return

	for index,column in conflicted_df.iterrows():
		# Variables:
		min_leven_first = sys.maxsize
		min_leven_second = sys.maxsize
		store_code_first = ''
		store_code_second = ''

		# First Apporach best match: (Name Vs Code match)
		for i,col in bhav_copy_df.iterrows():
			if column['market_rate'] == col['market_rate']:
				live_score = leven_score(column['symbol_name'][:-13],col['symbol_code'])
				# print(column['symbol_name'][:-13],col['symbol_name'])
				live_score -= abs(len(column['symbol_name'][:-13]) - len(col['symbol_code']))
				if(live_score < min_leven_first):
					min_leven_first = live_score
					store_code_first = col['symbol_code']
		if store_code_first == '':
			print("No match found for index: " + str(index) + " into BhavCopy file.")
			break

		# Second Apporach best match: (Name Vs DB Name)
		for i,col in isin_df.iterrows():
			live_score = leven_score(column['symbol_name'][:-13],col['symbol_name'])
			# print(column['symbol_name'][:-13],col['symbol_name'])
			live_score -= abs(len(column['symbol_name'][:-13]) - len(col['symbol_name']))
			# print(live_score)
			if(live_score < min_leven_second):
				min_leven_second = live_score
				store_code_second = col['symbol_code']
		if store_code_second == '':
			print("No match found for index: " + str(index) + " into isin_code(master table).")
			break

		# print("Matched Check: ",store_code_first + ' & '  + store_code_second)
		if(store_code_first == store_code_second):
			conflicted_df.loc[index,'symbol_code'] = store_code_first
		else:
			print("Stopping the Ingestion.")







	# print("length of exact match:",len(exactmatch_df.index))
	# print("conflicted Dataframe length:",len(conflicted_df.index))
	
	conflicted_df.to_csv("conflicted.csv")
	# print("conflicted Rows: ",conflicted_row)
	# print("Zero Matchs Rows: ",zero_match)

	# print("Result Number of rows:",len(input_df.index)-zero_match)
			
		
	
	
main()