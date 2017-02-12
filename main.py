import csv
import sqlparse
import re
def getData(f):
	fileName = f + '.csv'
	data = []
	with open(fileName,'rb') as file:
		dt = csv.reader(file, delimiter = ',')
		for row in dt:
			data.append(row)
	return data

def queryParse(query):
	parsed = sqlparse.parse(query);
	return parsed[0]
#	stmt = parsed[0]
#	print stmt.tokens[2]
#	print str(parsed[1])

def getSchema():
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

def selectQuery(cols, tables):
	tableName = tables[0]
	data = getData(tableName)
	columnsOfTable = schema[tableName]
	columnNumber = []
	for ele in cols:
		pos = columnsOfTable.index(ele)
		columnNumber.append(pos)
	reqData = []
#	print columnNumber
	for row in data:
		dt = []
		for i in range(len(row)):
			if i in columnNumber:
			#	print "i-> ",i 
				dt.append(row[i])
		reqData.append(dt)
#	print reqData
#	displayOutput(reqData , cols, tableName)
	return reqData

def displayOutput(data, cols, tableName):
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
	indexForColName = 2
	flagDistinct = 0
	columnToken = str(stmt.tokens[indexForColName])

	if str(stmt.tokens[indexForColName]) == "distinct":
		indexForColName = 4
		columnToken = str(stmt.tokens[indexForColName])
		flagDistinct = 1

	else:
		distinctCase2 = re.split('[( )]',str(stmt.tokens[indexForColName]))
		if distinctCase2[0] == "distinct":
			columnToken = distinctCase2[1]
			flagDistinct = 1

	tables = []
	tbllist = str(stmt.tokens[indexForColName + 4]).split(',')
	for ele in tbllist:
		tables.append(ele)
	if columnToken == '*':
		for ele in schema[tables[0]]:
			columns.append(ele)
	
	else:
		collist = columnToken.split(',')
		for ele in collist:
			columns.append(ele)

	selectData = selectQuery(columns,tables)
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

