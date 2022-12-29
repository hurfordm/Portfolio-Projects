import os
from Module.SQLiteCsvLoader import * 

# __author__ = Martin Hurford

daily_file_name     = './Data/SPY-daily.csv'
monthly_file_name   = './Data/SPY-monthly.csv'
sqlite_db           = './Data/data.db'
table_name          = 'test_table'

# Set the current working directory to the same as this file
# This fixes some Anaconda path issues in VS Code
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Start with a fresh database file
try:
    os.remove(sqlite_db)
except FileNotFoundError:
    pass

# Create our loader object and initialize with path to the database file.
# If the database file does not exist it will be created
loader = SQLiteCsvLoader()
loader.sqlite_database(sqlite_db)

# Pass in agrguments for our daily dataset
# If table exists it will be dropped and recreated
loader.source_file(daily_file_name)
loader.table_name('SPY_DAILY_DATA_DUMP')
# Execute the load process
loader.run()

# Do it again for our monthly dataset
loader.source_file(monthly_file_name)
loader.table_name('SPY_MONTHLY_DATA_DUMP')
# Execute the load process
loader.run()

""" 
Our database will now contain the following tables:

SPY_DAILY_DATA_DUMP
SPY_DAILY_DATA_DUMP_ERROR
&
SPY_MONTHLY_DATA_DUMP
SPY_MONTHLY_DATA_DUMP_ERROR

Any rows which fail the insert will be loaded into the associated '_ERROR' table
along with the error message and submitted SQL statement
i.e. rows which contain values that fail to match the defined column data types

All rows include the Source_Row_ID column to make it easier to check the data in source files should you need to.

Table DDL generated for daily_file_name (Source Row ID column contains comment to identify source file)
'STRICT' table constraint is used to override SQLite's default flexibility when it come to data types - https://www.sqlite.org/stricttables.html

CREATE TABLE SPY_DAILY_DATA_DUMP
( Source_Row_ID INTEGER PRIMARY KEY -- Source file ./Data/SPY-daily.csv
, Date_Loaded TEXT
, Date TEXT
, Open REAL
, High REAL
, Low REAL
, Close REAL
, Adj_Close REAL
, Volume INTEGER
) STRICT

CREATE TABLE SPY_DAILY_DATA_DUMP_ERROR
( Source_Row_ID INTEGER PRIMARY KEY -- Source file ./Data/SPY-daily.csv
, Date_Loaded TEXT
, Date ANY
, Open ANY
, High ANY
, Low ANY
, Close ANY
, Adj_Close ANY
, Volume ANY
, Error_Message TEXT
) STRICT

"""