import csv
import sqlparse
def getData():
	with open('table1.csv','rb') as file:
		data = csv.reader(file, delimiter = ',')
		for row in data:
			print row

def queryParse():
	str = "select * from a where sal>500;"
	parsed = sqlparse.parse(str);
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
	
if __name__ == "__main__":
	global schema;
	schema = dict()
	getSchema()

