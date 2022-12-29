
import csv
import os
import re
import sqlite3
from datetime import datetime

# __author__ = Martin Hurford

class SQLiteCsvLoader:

    """ Class enabling CSV files to be quickly dumped into SQLite for later analysis
    Only standard libraries used so may be deployed in a confined environment 
    Not strictly OOP but encapsulation organizes the code nicely"""

    def __init__(self):

        """ List our attributes and types for reference """

        self.sqlite_db_file_name    = ''
        self.sqlite_db_connection   = None
        self.sqlite_db_cursor       = None
        self.source_file_name       = ''
        self.csv_file_handle        = None
        self.target_table_name      = None  # Defaults to MY_DATA if not supplied
        self.error_table_name       = None  # self.target_table_name + '_ERROR'
        self.target_table_ddl       = ''
        self.error_table_ddl        = ''
        self.sample_rows            = [] 
        self.sample_rows_string     = ''
        self.header_row_flag        = False
        self.column_headers         = []
        self.csv_dialect            = None
        self.csv_reader             = None
        self.column_data_types      = {}


    def sqlite_database(self, db_file=False):

        """ Check db file path and set-up db connection and cursor """

        if not db_file:
            try:
                raise RuntimeError('\nError: SQLite database file name required!\nUsage: class SQLiteCsvLoader.sqlite_database(db_file_path)\n')
            except Exception as e:
                print(e)
                exit()

        if not os.path.exists(os.path.dirname(db_file)):
            try:
                raise FileNotFoundError(f'\nError: Directory path not found for SQLite database file {os.path.abspath(db_file)}\n')
            except Exception as e:
                print(e)
                exit()

        self.sqlite_db_file_name = db_file

        try:
            self.sqlite_db_connection  = sqlite3.connect(self.sqlite_db_file_name)
            self.sqlite_db_cursor = self.sqlite_db_connection.cursor()
        except sqlite3.Error as e:
            print(e)
            exit()


    def source_file(self, source_file_name=False):

        """" Set/Return the source file name, raise a RuntimeError if not provided """

        if source_file_name:
            self.source_file_name = source_file_name
        elif not source_file_name and not self.source_file_name:
            try:
                raise RuntimeError('\nError: No source data file defined!\nUsage: class SQLiteCsvLoader.source_file(file_name)\n')
            except Exception as e:
                print(e)
                exit()

        return self.source_file_name


    def table_name(self, table_name='MY_DATA'):

        """ Set/Return the target table name, set default name MY_DATA if not provided """

        if table_name != 'MY_DATA':
            self.target_table_name = table_name.replace('-','_').replace(' ','_').upper()
        elif not self.target_table_name and table_name == 'MY_DATA': 
            self.target_table_name = table_name

        self.error_table_name = self.target_table_name + '_ERROR'    
        
        return self.target_table_name


    def run(self):

        """ Check for prerequisites then execute the analyzing and load processes """

        # Reset flag in case run() is executed more than once
        self.header_row_flag = False

        if not self.sqlite_db_file_name:
            self.sqlite_database()

        if not self.source_file_name:
            self.source_file()

        if not self.target_table_name:
            self.table_name()

        self.__sample_rows()
        self.__set_column_headers()
        self.__analyze_column_data_types()
        self.__choose_column_data_types()
        self.__create_table_ddl()
        self.__create_table()
        self.__insert_data_into_table()


    def __sample_rows(self):
        
        """ Sample the source file rows and allow csv module to sniff out the file format and select the correct input dialect """

        self.csv_file_handle    = open(self.source_file_name, encoding='utf-8')
        self.sample_rows        = [self.csv_file_handle.readline() for i in range(0,10)]
        self.sample_rows_string = str().join(self.sample_rows) 
        self.csv_dialect        = csv.Sniffer().sniff(self.sample_rows_string)

        # Return read position back to beginning of csv file
        self.csv_file_handle.seek(0)

        
    def __set_column_headers(self):

        """ Sniff first row from file for included column headers, generate defaults if not found """

        self.column_headers = []

        first_row = self.sample_rows[0]
        first_row = first_row.replace('\n','')
        headers = first_row.split(self.csv_dialect.delimiter)

        if csv.Sniffer().has_header(self.sample_rows_string):
            self.header_row_flag = True
            for header in headers:
                header = header.replace(self.csv_dialect.quotechar,'')
                header = header.title()
                header = re.sub('[\.-]','_',header)
                header = re.sub(' +','_',header)
                self.column_headers.append(header)
        else:
            self.column_headers = ['Column_' + str(index + 1) for index, value in enumerate(headers)]


    def __analyze_column_data_types(self):

        """ Analyze data rows to try and determine column data types for SQLite table DDL generation
            Works well for values that are obviously INTEGER or FLOAT type. All other type are defined as TEXT
            i.e. Date fields, Comma seperated INTEGERS e.g. 1,234,567
            Good enough to get the data into SQLIte for further analysis
        """
        self.column_data_types = []

        self.csv_reader = csv.DictReader(self.csv_file_handle, fieldnames=self.column_headers, dialect=self.csv_dialect)

        if self.header_row_flag:
            next(self.csv_reader)

        column_data_types = {}
        i = 0

        for row in self.csv_reader:

            # Sample every 10th row for 1000 rows (100 rows total)
            if i % 10 == 0 and i < 1000:

                for column_name, value in row.items():

                    if column_name in column_data_types:

                        data_type = self.__get_data_type(value)

                        if data_type in column_data_types[column_name]:
                            
                            column_data_types[column_name][data_type] += 1
                        else:
                            column_data_types[column_name][data_type] = 1
                    else:
                        column_data_types[column_name] = { self.__get_data_type(value) : 1 }

            i += 1

        self.column_data_types = column_data_types


    def __get_data_type(self,value):

        if self.__is_int(value):
            return 'INTEGER'
        elif self.__is_float(value):
            return 'REAL'
        else:
            return 'TEXT'


    def __is_int(self,value):
        try:
            if(int(value)):
                return True
        except ValueError:
            return False


    def __is_float(self,value):
        try:
            if(float(value)):
                return True
        except ValueError:
            return False


    def __choose_column_data_types(self):

        final_data_types = {}

        for column_name, data_types in self.column_data_types.items():
            final_data_types[column_name] = max(data_types, key=data_types.get)
            
        self.column_data_types = final_data_types


    def __create_table_ddl(self):

        """ Generate target table and error table DDL """

        for table_type in ['','_ERROR']:

            ddl =  f'DROP TABLE  IF EXISTS {self.target_table_name}{table_type};\n'
            ddl += f'CREATE TABLE {self.target_table_name}{table_type}\n'
            ddl += f'( Source_Row_ID INTEGER PRIMARY KEY -- Source file {self.source_file_name}\n'
            ddl += ', Date_Loaded TEXT\n'

            for column_name, data_type in self.column_data_types.items():

                if table_type == '_ERROR':
                    ddl += f', {column_name} ANY\n'
                else:
                    ddl += f', {column_name} {data_type}\n'

            if table_type == '_ERROR':
                ddl += ', Error_Message TEXT\n'


            ddl += ') STRICT;\n'

            if table_type == '':
                self.target_table_ddl = ddl
            else:
                self.error_table_ddl = ddl


    def __create_table(self):

        """ Create our target and error tables """

        try:
            self.sqlite_db_cursor.executescript(self.target_table_ddl)
        except sqlite3.Error as e:
            print('\nCreate Table Error:', self.target_table_name,'\n')
            print('\n',e,'\n')
            print('\nTarget Table DDL:\n',self.target_table_ddl,'\n')
            exit()

        try:
            self.sqlite_db_cursor.executescript(self.error_table_ddl)
        except sqlite3.Error as e:
            print('\nCreate Table Error:', self.error_table_name,'\n')
            print('\n',e,'\n')
            print('\nError Table DDL:\n',self.error_table_ddl,'\n')
            exit()

    def __insert_data_into_table(self):

        """ Normally we would bind data values to the SQL insert statement using the '?' placeholders.
        However.....we are not worried about 'SQL Injection' attacks in this instance. 
        By generating the complete SQL statement we can capture and insert it into the error table 
        when an insert fails to give greater context to the error message."""
        
        self.csv_file_handle.seek(0)

        if self.header_row_flag:
            self.csv_file_handle.readline()

        todays_date = str(datetime.date(datetime.now()))
        
        # Source Row ID
        i = 1

        self.sqlite_db_cursor.execute('BEGIN;')

        for row in self.csv_reader:

            columns = [column_name for column_name in row.keys() if column_name != None]
            columns.insert(0,'Date_Loaded')
            columns.insert(0,'Source_Row_ID')
            columns_string = ', '.join(columns).replace('\n','')

            values = []
            for value, data_type in zip(row.values(),self.column_data_types.values()):

                if(data_type == 'TEXT'):
                    # Escape single quotes in text values
                    value = '' if value is None else value.replace("'","''") 
                    values.append(f"'{value}'")
                else:
                    values.append(value)

            values.insert(0,f"'{todays_date}'")
            values.insert(0,str(i))
            values_string = ', '.join(values)

            # Use format here instead of f'strings' so we can use sql_text again for any error row insertion
            sql_text = 'INSERT INTO {table_name} ({columns}) VALUES ({values});'
            sql = sql_text.format(table_name=self.target_table_name,columns=columns_string,values=values_string)

            try:
                self.sqlite_db_cursor.execute(sql)

            except sqlite3.Error as e:
                columns.append('Error_Message')
                columns_string = ', '.join(columns)

                values.append(f"""[ERROR: {str(e)}] [SQL: {sql.replace("'",'"')}]""")
                values = [''.join(["'",value.replace('\'',''),"'"]) for value in values]
                values_string = ', '.join(values)

                sql = sql_text.format(table_name=self.error_table_name,columns=columns_string,values=values_string)

                try:
                    self.sqlite_db_cursor.execute(sql)

                except sqlite3.Error as e:
                    print(e)

            i += 1

        self.sqlite_db_cursor.execute('COMMIT;')


    def __del__(self):
        self.csv_file_handle.close()
        self.sqlite_db_connection.close()
