"""
author: evan fedorko, evanjfedorko@gmail.com
date: 10/2021

This script took coal minining employment data (reported by mine and county) and summed it all
up to county level. it also deals with some problematic naming in the source data. matching uses
regex in a few spots.

pseudo code of process follows

import pandas as pan

declare vars

make list of files in dir

loop through each file to:
	read in starting on line 4 of "Hist_Coal_Prod" sheet
	get working year
	read state and parse (stop before ()s) to create state string (use func)
	read county
	combine to "county_state"
	create column for county_state and that record's tons and avg employees
use the df from the loop to sum values to county_state (reduce rows w/groupby I think)
add working year to each line(?)
move to next file/year

combine annual DFs into one DF w/column per year-employment and year-production (also group by?)

write output
"""

import pandas as pan
import os
import re
import numbers
import merge_csv


# processing variables
cwd = os.getcwd()

fipsLookup = cwd + r'\Lower48CountiesTableFIPS_Crosswalk.xlsx'

fipsLookupSheet = 'final'

fileList = []

for root, dirs, files in os.walk(cwd):
	for file in files:
		if file.endswith('.xls'):
			fileList.append(os.path.join(root, file))


def main():
	fipsDF = pan.read_excel(fipsLookup, sheet_name=fipsLookupSheet, header=0, index_col=3)
	for file in fileList:
		year = re.search(r'2\d\d\d', file)[0]
		df = pan.read_excel(file, sheet_name='Hist_Coal_Prod', skiprows=[0, 1, 2], header=0)
		cols = df.columns
		# remove spaces and () from column names
		specialChars = ' ()'
		for specialChar in specialChars:
			cols = cols.map(lambda x: x.replace(specialChar, '') if isinstance(x, str) else x)
		# rename columns
		df.columns = cols
		# add county_state column and fips column
		df['CountyState'] = ''
		df['fips'] = 0
		# print(df.columns)
		for i, row in df.iterrows():
			county = str(df.MineCounty[i])
			state = functionToFixState(str(df.MineState[i]))
			county_state = county + '_' + state
			# df.loc says "at index i for column 'CountyState' set equal to"
			df.loc[i, 'CountyState'] = county_state
			# clean up junk values with 0s
			df.loc[i, 'Productionshorttons'] = returnZeroForNonNumber(df.Productionshorttons[i])
			df.loc[i, 'AverageEmployees'] = returnZeroForNonNumber(df.AverageEmployees[i])
			try:
				fipsVal = fipsDF.at[county_state, 'fips2']
				df.loc[i, 'fips'] = fipsVal
			except Exception:
				df.loc[i, 'fips'] = -9999
		# groupby county_state
		# either of these will return the sum of production tons and employees
		# but the first is out of date with the final format, which includes other stuff
		# df2 = df.groupby(['CountyState'])['Productionshorttons', 'AverageEmployees'].apply(lambda x: x.astype(int).sum())
		df2 = df.groupby(['CountyState', 'fips']).agg({'Year': 'min', 'Productionshorttons': 'sum', 'AverageEmployees': 'sum'})
		writeToCsv(df2, year + '_summary.csv')
	# print(df2)
	# print(df.iloc[0])


def functionToFixState(target):
	# all problem strings are some variation of "State (something)"
	# we want to find the first " (" and slice everything before that
	string = target
	if re.search(r' \(', string):
		mat = re.search(r' \(', string)
		index = mat.start()
		state = str(string[0:index])
		return state
	else:
		return string


def returnZeroForNonNumber(x):
	# checks that value is a number
	if isinstance(x, numbers.Number):
		return x
	else:
		return 0


def writeToCsv(targetDF, output):
	targetDF.to_csv(cwd + "\\" + output)


# main program
main()
merge_csv.merge(cwd)
