import csv
import sqlparse
def getData(f):
	fileName = f + '.csv'
#	print fileName
	data = []
	with open(fileName,'rb') as file:
		dt = csv.reader(file, delimiter = ',')
		for row in dt:
			data.append(row)
#	print data	
	return data

def queryParse():
	query = "select * from a where sal>500;"
	parsed = sqlparse.parse(query);
	stmt = parsed[0]
	print stmt.tokens[-1]
	#print str(parsed[1])

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
	displayOutput(data , cols, tableName)

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

if __name__ == "__main__":
	global schema;
	schema = dict()
	getSchema()
	selectQuery(['A','B','C'],['table1'])
#	getData()

