import csv
import sqlparse
import re
def getData(f): #read table data from file
	fileName = f + '.csv'
	data = []
	with open(fileName,'rb') as file:
		dt = csv.reader(file, delimiter = ',')
		for row in dt:
			data.append(row)
	return data

def queryParse(query): #parse the query
	parsed = sqlparse.parse(query);
	return parsed[0]
#	stmt = parsed[0]
#	print stmt.tokens[2]
#	print str(parsed[1])

def getSchema(): #get schema of tables from the metadata file
	file = open('metadata.txt','rb')
	flag = 0
	tableName=""
	for row in file:
		if row.strip() == "<begin_table>":
			flag = 1

		elif flag == 1:
			tableName = row.strip()
			schema[tableName] = []
			flag = 0

		elif row.strip() != "<end_table>" and flag == 0:
			schema[tableName].append(row.strip())
#	print schema

def selectQuery(cols, tables): # implements the select part of the query
	tableName = tables[0] 
	data = getData(tableName) # get table data
	columnsOfTable = schema[tableName] #the columns in the table
	columnNumber = []
	for ele in cols:
		pos = columnsOfTable.index(ele)
		columnNumber.append(pos)	#get the column number for the required fields
	reqData = []
	for row in data:	#get data for the required fields only
		dt = []
		for i in range(len(row)):
			if i in columnNumber:
				dt.append(row[i])
		reqData.append(dt)
#	displayOutput(reqData , cols, tableName)
	return reqData

def displayOutput(data, cols, tableName): #displays the required final output
	lineWidth = 20
	for it in cols:
		header = tableName + "." + it
		print header.ljust(lineWidth),
	print
	for row in data:
		for ele in row:
			print ele.ljust(lineWidth),
		print

def processQuery(query):
	stmt = queryParse(query)

	columns = []
	indexForColName = 2 #index of column names in the parsed tokens
	flagDistinct = 0	#to check if distinct values need to be output
	columnToken = str(stmt.tokens[indexForColName])	#token with columnnames

	if str(stmt.tokens[indexForColName]) == "distinct": #case : distinct <columnname>
		indexForColName = 4	#if distinct <column name>, then token index is 4
		columnToken = str(stmt.tokens[indexForColName])
		flagDistinct = 1

	else: #case : distinct(<columnname>) or aggregate functions
		distinctCase2 = re.split('[( )]',str(stmt.tokens[indexForColName]))	
		if distinctCase2[0] == "distinct":
			columnToken = distinctCase2[1]
			flagDistinct = 1

	#get tablenames
	tables = []
	tbllist = str(stmt.tokens[indexForColName + 4]).split(',')
	for ele in tbllist:
		tables.append(ele)

	#retrieve the columnames from the column token
	if columnToken == '*':
		for ele in schema[tables[0]]:
			columns.append(ele)
	
	else:
		collist = columnToken.split(',')
		for ele in collist:
			columns.append(ele)

	#get data for required columns
	selectData = selectQuery(columns,tables)

	#delete repeated data in case of distinct
	reqData = []
	if flagDistinct == 1:
		for ele in selectData:
			if ele not in reqData:
				reqData.append(ele)
	displayOutput(reqData, columns, tables[0])


if __name__ == "__main__":
	global schema;
	schema = dict()
	print "prompt> "
	query = raw_input()
	getSchema()
	processQuery(query)

