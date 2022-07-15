#Making all necessary imports for the code
from re import T
from jugaad_data.nse import bhavcopy_fo_save
import pandas as pd
from jugaad_data.holidays import holidays
from random import randint
import time, os
import numpy as np
import datetime
import enchant



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


def tb_df_format():
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


def bhav_fut_df_format():
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


def main():
	# Formated df of TB1
	tb_df = tb_df_format()
	print("TB dataframe length:",len(tb_df.index))
	# print("TB Dataframe:",tb_df.head(2))
	tb_df.to_csv("tb.csv")
	#Formated df of FUTURES for that date:
	bhav_fut_df = bhav_fut_df_format()
	# bhav_fut_df.to_csv("bhav.csv")
	# print("BhavCopy: ",bhav_fut_df.head(2))

	counter_check = 0
	for index,column in tb_df.iterrows():
		count = (column['market_rate'] == bhav_fut_df['market_rate']).sum()
		if count == 0 :
			# expiry_check = (expirydate(column['symbol_name']) == bhav_fut_df['expiry_date']).sum()
			# print("Before Check:",index,count,column['symbol_name'])
			counter_check += 1
	print("Result Number of rows:",len(tb_df.index)-counter_check)
			
		
	
	result_df = pd.merge(tb_df,
				 bhav_fut_df,
				 on='market_rate')
	print("Before Dropping rows:",len(result_df))
	# print("Result Df first:",result_df.head(1))
	result_df.to_csv("result.csv")
	# print("Result one:",result_df.columns)
	# final_df = pd.DataFrame()

	final_df = result_df[ (result_df['expiry_date'] == result_df['symbol_name'].str[-10:]) ]
	final_df = final_df.drop_duplicates(keep=False)
	# for index,column in final_df.iterrows():
	# 	count = (column['market_rate'] == bhav_fut_df['market_rate']).sum()
	# 	if count != 1:
	# 		# expiry_check = (expirydate(column['symbol_name']) == bhav_fut_df['expiry_date']).sum()
	# 		print("After:",index,count,column['symbol_name'])
	# 		new_final_df = final_df.drop(final_df[].index)


	
	# print("Final One",final_df.head(2))
	for index,column in final_df.iterrows():
		final_df.loc[index,'leven_score'] = leven_score(column['symbol_code'],column['symbol_name'])
	# final_df.to_csv("final.csv")
	# print("Length",len(final_df.index))



	row_iterator = final_df.iterrows()
	next_one = next(row_iterator)
	count = 0
	for index,column in final_df.iterrows():
		count += 1
		if count >= len(final_df.index):
			break
		next_one = next(row_iterator)
		# print("NExt one:",column['symbol_name'],next_one[1]['symbol_name'])
		if (column['symbol_name'] == next_one[1]['symbol_name']) and (column['market_rate'] == next_one[1]['market_rate']) and (column['average_unit_cost'] == next_one[1]['average_unit_cost']):
			if(column['leven_score'] > next_one[1]['leven_score']):
				# print("Drop",index)
				final_df = final_df.drop(index)
			else:
				final_df = final_df.drop(index+1)
				# print("Drop:",index+1)





	# final_df = final_df.drop(final_df[ ((final_df['symbol_name'] == final_df['symbol_name'].shift(-1)) 
	# & (final_df['symbol_code'] != final_df['symbol_code'].shift(-1))
	
	# & (final_df['leven_score'] > final_df['leven_score'].shift(-1)))].index)
	print(final_df.head(2))
	# final_df = final_df.drop('leven_score',inplace=True,axis=1)
	print("After Dropping Duplicates:",len(final_df.index))
	final_df.to_csv("thisone.csv")
	# final_df.to_csv("final.csv")
	# print(final_df.head(2))
	
	

main()