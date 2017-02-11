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
	file = open('metadata.txt','r')
	for row in file:
		if row != '\n':
			print row
	print file
	print type(file)
	
if __name__ == "__main__":
	global schema;
	schema = dict()
	getSchema()

