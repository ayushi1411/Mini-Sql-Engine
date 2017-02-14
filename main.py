import csv
import sqlparse
import re
import sys
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
				dt.append(int(row[i]))
		reqData.append(dt)
	return reqData

def displayOutput(data, cols, tableName): #displays the required final output
	lineWidth = 20
	i = 0
	for it in cols:
		header = tableName[i] + "." + it
		print header.ljust(lineWidth),
		i+=1
	print
	for row in data:
		for ele in row:
			print str(ele).ljust(lineWidth),
		print

def processQuery(query):
	stmt = queryParse(query)

	columns = []
	indexForColName = 2 #index of column names in the parsed tokens
	flagDistinct = 0	#to check if distinct values need to be output
	columnToken = str(stmt.tokens[indexForColName])	#token with columnnames
	flagAggregate = 0

	if str(stmt.tokens[indexForColName]) == "distinct": #case : distinct <columnname>
		indexForColName = 4	#if distinct <column name>, then token index is 4
		columnToken = str(stmt.tokens[indexForColName])
		flagDistinct = 1

	else: #case : distinct(<columnname>) or aggregate functions
		case2 = re.split('[( )]',str(stmt.tokens[indexForColName]))	
		func = case2[0]
		
		if func == "distinct":
			flagDistinct = 1
			columnToken = case2[1]

		elif func == "max" or func == "min" or func == "sum" or func == "avg":
			flagAggregate = 1
			columnToken = case2[1]

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

	if len(tables) == 1:
		#get data for required columns
		selectData = selectQuery(columns,tables)

		#delete repeated data in case of distinct
		reqData = []
		if flagDistinct == 1:
			for ele in selectData:
				if ele not in reqData:
					reqData.append(ele)

		elif flagAggregate == 1:
			if(len(selectData[0]) > 1):
				print "only one column allowed in aggregate functions"
		
			else : 			
				l1 = []
				for ele in selectData:
					for val in ele:
						l1.append(val)


				if func == "max":
					l = []
					l.append(max(l1)) 
					reqData.append(l)

				elif func == "min":
					print l1
					l = []
					l.append(min(l1)) 
					reqData.append(l)

				elif func == "sum":
					l = []
					l.append(sum(l1)) 
					reqData.append(l)

				elif func == "avg":
					l = []
					l.append(sum(l1)/(float)(len(l1))) 
					reqData.append(l)
		else:
			for ele in selectData:
				reqData.append(ele)
		tables = tables * len(columns)

#where on one table and one condition
		whereClause = str(stmt.tokens[-1])
	#	print whereClause
		whereClause1 = whereClause.split(' ')
	#	print whereClause1
		if whereClause1[0] == "where":
		#	op = ['>', '<', '=', '<=', '>=', '!=']
		#	whereClause = re.split('[> < = <= >= !=]',whereClause1)
		#	if '' in whereClause:
		#		whereClause.remove('')
			clm = whereClause1[1]
			op = whereClause1[2]
			num = whereClause1[3].split(";")[0]
			num = int(num)

			columnstemp = []
			for ele in schema[tables[0]]:
				columnstemp.append(ele)
			postemp = schema[tables[0]].index(clm)
			sdata = selectQuery(columnstemp,tables)
			rownumber = []
			i = 0
			for ele in sdata:
				cond = str(ele[postemp]) + str(op) + str(num)
				if eval(cond) == True:
					rownumber.append(i)
				i+=1
			tempdata = []
			for i in range(len(reqData)):
				if i in rownumber:
					tempdata.append(reqData[i])

			reqData = tempdata

	else:
		whereClause = str(stmt.tokens[-1])
		whereClause = whereClause.split(' ')
		#join
		if len(tables) == 2:
			if len(whereClause) == 2:
				joinStmt = whereClause[1]
				whereClause = whereClause[1].split('=')
				clm1 = whereClause[0]
				clm2 = whereClause[1]
				tbl1 = clm1.split(".")[0]
				clm1 = clm1.split(".")[1]
				tbl2 = clm2.split(".")[0]
				clm2 = clm2.split(".")[1]
				clm2 = clm2.split(";")[0]
				ct1 = []
				ct2 = []
				if columnToken == '*':
					for ele in schema[tables[1]]:
						if ele not in columns:
							columns.append(ele)
				for c in columns:
					if c in schema[tables[0]]:
						ct1.append(c)
					if c in schema[tables[1]]:
						ct2.append(c)
				if clm2 in ct2:
					ct2.remove(clm2)
				table1 = []
				table1.append(tables[0])	
				table2 = []
				table2.append(tables[1])	
				selectData1 = selectQuery(ct1,table1)
				selectData2 = selectQuery(ct2,table2)
				columnsOfTable = schema[tables[0]] #the columns in the table
				col = []
				for ele in columnsOfTable:
					col.append(ele)
				dt1 = selectQuery(col, table1)

				columnsOfTable = schema[tables[1]] #the columns in the table
				col = []
				for ele in columnsOfTable:
					col.append(ele)
				dt2 = selectQuery(col, table2)

				pos1 = schema[tables[0]].index(clm1)
				pos2 = schema[tables[1]].index(clm2)

				rowNumbers1 = []
				rowNumbers2 = []
				i = 0
				for ele1 in dt1:
					j=0
					for ele2 in dt2:
						if ele1[pos1] == ele2[pos2]:
							rowNumbers1.append(i)
							rowNumbers2.append(j)
						j+=1
					i+=1
				reqData = []
				for i in range(len(rowNumbers1)):
					temp = []
					for e in selectData1[rowNumbers1[i]]:
						temp.append(e)
					for e in selectData2[rowNumbers2[i]]:
						temp.append(e)
					reqData.append(temp)
			tables = table1 * len(ct1)
			temptb = []
			temptb = table2[0] * len(ct2)
			tables.append(temptb)
#			print reqData
	displayOutput(reqData, columns, tables)


if __name__ == "__main__":
	global schema;
	schema = dict()
#	print "prompt> "
#	query = raw_input()
	query = (' ').join(sys.argv[1:])
#	print query
	getSchema()
	processQuery(query)

'''		whereClause = str(stmt.tokens[-1])
		whereClause1 = whereClause.split(' ')
		op = ['>', '<', '=', '<=', '>=', '!=']
		whereClause = re.split('[> < = <= >= !=]',whereClause1)
		if '' in whereClause:
			whereClause.remove('')
		clm = whereClause[0]
		num = int(whereClause[1])
		columnstemp = []
		for ele in schema[tables[0]]:
			columnstemp.append(ele)
		postemp = schema[tables[0]].index(clm)
		sdata = selectQuery(columnstemp,tables)
		'''