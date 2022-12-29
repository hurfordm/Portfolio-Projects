# Load CSV Data into SQLite Database
An example of how to quickly dump csv datasets into an SQLite database using only the standard library

## How to use
- Have Python >= 3.9 installed on your machine
- Clone or download this repository
- In a shell, execute the `load_csv.py` script with Python 3
- Provide a csv file and modify `load_csv.py` to load your own data

## Contents
- __load_csv.py__: Script providing the parameters to create the SQLiteCsvLoader object and execute the load process
- __Module__: Folder containing Python code for class _SQLiteCsvLoader_
- __Data__: Folder containing csv datasets of daily and monthly S&P 500 ETF prices and the target SQLite database
  - __SPY-daily.csv__
  - __SPY-monthly.csv__
  - __data.db__
- __README.md__: The document you are currently reading

## Background
In an environment where you are free to install and use the libraries of your choosing you would complete similar tasks using libraries like [Pandas](https://pandas.pydata.org/) and/or [SQLAlchemy](https://www.sqlalchemy.org/). _However, that probably wouldn't make a great example for this portfolio!_

In this contrived scenario, the Python environment is limited to the [Python Standard Library](https://docs.python.org/3/library/index.html). The purpose of this project is to quickly enable the dumping of csv datasets into a database table so it can be further analyzed and processed using SQL. Table DDL is generated with the correct datatypes derived from sampling the dataset. Displays proficiency accessing and analyzing data files, auto generating SQL & DDL statments, and loading SQL databases with Python 3.

### Goals for this project
- Sample the csv file to:
  - Detect the presence of a header row - No header row? Generate column names for target table Column_1 -> Column_n 
  - Determine the data types of column values (limited to INTEGER, FLOAT and TEXT types) 
- Generate 'Create Table' DDL for target and error tables using information derived from csv sampling. Use 'Strict' tables, see comments in `load_csv.py`. 
- Load csv data into target table. Include source file row id to ease referencing back to the file. Load date is also to be included to provide further context to the data, i.e. to differentiate rows in the case where more rows are inserted at a later date. 
- Rows that fail insert to the target table should be inserted into the error table along with the error messsage and the offending SQL statement.

### To do
- Expand on data type detection to include dates and other common field formats