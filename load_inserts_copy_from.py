# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options

import time
import psycopg2
import argparse
import re
import csv
import io

DBname = "datastorage"
DBuser = "datastorageuser"
DBpwd = "datasausagePASSWORD"
TableName = 'CensusData'
Datafile = "filedoesnotexist"  # name of the data file to be loaded
CreateDB = False  # indicates whether the DB table should be (re)-created
Year = 2015


def initialize():
	global Year

	parser = argparse.ArgumentParser()
	parser.add_argument("-d", "--datafile", required=True)
	parser.add_argument("-c", "--createtable", action="store_true")
	parser.add_argument("-y", "--year", default=Year)
	args = parser.parse_args()

	global Datafile
	Datafile = args.datafile
	global CreateDB
	CreateDB = args.createtable
	Year = args.year

# read the input data file into a list of row strings
# skip the header row
def readdata(fname):
	print(f"readdata: reading from File: {fname}")
	with open(fname, mode="r") as fil:
		dr = csv.DictReader(fil)
		headerRow = next(dr)

		rowlist = []
		for row in dr:
			row.update({'Year': Year})
			row.move_to_end('Year', last=False)
			rowlist.append(row)

	return rowlist


# connect to the database
def dbconnect():
	connection = psycopg2.connect(
		host="localhost",
		database=DBname,
		user=DBuser,
		password=DBpwd,
	)
	connection.autocommit = True
	return connection

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):

	with conn.cursor() as cursor:
		cursor.execute(f"""
			DROP TABLE IF EXISTS {TableName};
			CREATE TABLE {TableName} (
				Year                INTEGER,
			  CensusTract         NUMERIC,
				State               TEXT,
				County              TEXT,
				TotalPop            INTEGER,
				Men                 INTEGER,
				Women               INTEGER,
				Hispanic            DECIMAL,
				White               DECIMAL,
				Black               DECIMAL,
				Native              DECIMAL,
				Asian               DECIMAL,
				Pacific             DECIMAL,
				Citizen             DECIMAL,
				Income              DECIMAL,
				IncomeErr           DECIMAL,
				IncomePerCap        DECIMAL,
				IncomePerCapErr     DECIMAL,
				Poverty             DECIMAL,
				ChildPoverty        DECIMAL,
				Professional        DECIMAL,
				Service             DECIMAL,
				Office              DECIMAL,
				Construction        DECIMAL,
				Production          DECIMAL,
				Drive               DECIMAL,
				Carpool             DECIMAL,
				Transit             DECIMAL,
				Walk                DECIMAL,
				OtherTransp         DECIMAL,
				WorkAtHome          DECIMAL,
				MeanCommute         DECIMAL,
				Employed            INTEGER,
				PrivateWork         DECIMAL,
				PublicWork          DECIMAL,
				SelfEmployed        DECIMAL,
				FamilyWork          DECIMAL,
				Unemployment        DECIMAL
		 	);	
		 	ALTER TABLE {TableName} ADD PRIMARY KEY (CensusTract);
		 	CREATE INDEX idx_{TableName}_State ON {TableName}(State);
		""")

		print(f"Created {TableName}")


def loadTableCopy(conn, censusDict):
	# Replace empty string with '-1' to avoid number type errors
	for item in censusDict:
		for k,v in item.items():
			if v == '':
				item[k] = '-1'

	# Construct stream from dict
	data = io.StringIO()
	for item in censusDict:
		line = ",".join(item.values())
		line += "\n"
		n = data.write(line)

	cur = conn.cursor()
	start = time.perf_counter()
	data.seek(0)
	cur.copy_from(data, "CensusData", sep=',')
	elapsed = time.perf_counter() - start
	print(f'Finished Loading into Table using copy_from. Elapsed Time: {elapsed:0.4} seconds')


def main():
	initialize()
	conn = dbconnect()
	rlis = readdata(Datafile)

	if CreateDB:
		createTable(conn)

	loadTableCopy(conn, rlis)


if __name__ == "__main__":
	main()
