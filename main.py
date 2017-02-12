import csv
import sqlparse
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
	displayOutput(reqData , cols, tableName)

def displayOutput(data, cols, tableName):
	lineWidth = 20
	for it in cols:
		header = tableName+"."+it
		print header.ljust(lineWidth),
	print
	for row in data:
		for ele in row:
			print ele.ljust(lineWidth),
		print

def processQuery(query):
	stmt = queryParse(query)
	columns = []
	if stmt.tokens[2] == '*':
		columns.append('*')
	else:
	#	print type(stmt.tokens[2])
		collist = str(stmt.tokens[2]).split(',')
		for ele in collist:
			columns.append(ele)

	tables = []
	tbllist = str(stmt.tokens[6]).split(',')
	for ele in tbllist:
		tables.append(ele)

	selectQuery(columns,tables)


if __name__ == "__main__":
	global schema;
	schema = dict()
	print "prompt> "
	query = raw_input()
	getSchema()
	processQuery(query)

