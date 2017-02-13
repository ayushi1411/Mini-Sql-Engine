#!/bin/bash
python main.py "select sum(B) from table2;"
python main.py "select B,D from table2;"
python main.py "select B from table2;"
python main.py "select distinct B from table2;"
python main.py "select A from table1;"
python main.py "select * from table1;"
python main.py "select distinct(B) from table2;"