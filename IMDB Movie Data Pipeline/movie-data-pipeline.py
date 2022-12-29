import re
import os
import glob
import gzip
import shutil
import datetime
import requests
import subprocess
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# __author__ - Martin Hurford
#===============================================================================

def myprint(*args):

    """Print out progress messages with the current timestamp"""

    print(datetime.datetime.now(),' : ',*args)


def get_source_data_urls(target_url):

    """Extract links to datasets from the target url"""

    myprint(f'Downloading {target_url}')

    response = requests.get(target_url)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # not a success code 200
        print("Error: " + str(e))

    html = response.content
    soup = BeautifulSoup(html,'lxml')
    hrefs = [ element['href'] for element in soup.find_all(href=re.compile(".tsv.gz"))]

    myprint("Extracted file urls:\n\t","\n\t".join(hrefs))

    return hrefs

#===============================================================================

def generate_file_and_table_names(dataset_urls):

    """Generate file and table names (from the source dataset names) that we will use later"""

    data = []

    for url in dataset_urls:

        zip_file  = urlparse(url).path.replace('/','')
        flat_file = zip_file.replace('.gz','')
        staging_table = 'STAGING_' + flat_file.replace('.tsv','').replace('.','_').upper()
        data.append({
            'url' : url
        ,   'zip_file' : zip_file
        ,   'flat_file' : flat_file
        ,   'staging_table' : staging_table
        })

    return data

#===============================================================================

def download_dataset(url,local_file):

    """Download the dataset from url to local_file using chunks to keep memory usage under control"""

    myprint(f'Downloading dataset from {url}')

    try:
        with open(local_file,'wb') as zip_file:
            for chunk in requests.get(url, stream=True).iter_content(chunk_size=1024*10):
                if chunk:
                    zip_file.write(chunk)
    except Exception as e:

        myprint(f'An error occured while downloading {url}',e)

    myprint(f'{local_file} downloaded successfully')

#===============================================================================

def uncompress_dataset(compressed_file,uncompressed_file):

    """Uncompress gzipped file"""

    myprint(f'Uncompressing {compressed_file}')

    try:

        with gzip.open(compressed_file,'rb') as zip_file:
            with open(uncompressed_file,'wb') as flat_file:
                shutil.copyfileobj(zip_file,flat_file)

    except Exception as e:

        myprint(f'An error occured while uncompressing {compressed_file}',e)

    myprint(f'{compressed_file} successfully uncompressed to {uncompressed_file}')

#===============================================================================

def execute_sql_file(database,sql_file):

    """Execute data processing SQL file using sqlite3 command line utility"""

    myprint(f'Executing {sql_file}')

    args = ['sqlite3', f'{database}'
    ,f".read '{sql_file}'"
    ,'.quit']

    result = subprocess.run(
        args, text=True, capture_output=True
    )
    
    if result.returncode != 0:
        commands = "\n" + " ".join(args[0:2]) + "\n" + "\n".join(args[2:]) + "\n"
        raise subprocess.CalledProcessError(result.returncode,commands + result.stderr)

    myprint(f'{sql_file} successfully executed')

#===============================================================================

def create_staging_tables(database):

    """Execute file containing all the staging tables DDL"""

    sql_file = '../SQL/create_staging_tables.sql'

    execute_sql_file(database,sql_file)

#===============================================================================

def import_dataset(database,table_name,tab_delimited_file):

    """Import dataset into staging table using sqlite3 command line utility"""

    myprint(f'Importing data from {tab_delimited_file} into {table_name}')

    cache_size_mb = 500
    cache_size_bytes = cache_size_mb * 1024

    args = ['sqlite3', f'{database}'
    ,'PRAGMA journal_mode = OFF;'
    ,'PRAGMA synchronous = OFF;'
    ,'PRAGMA temp_store = 2;'
    ,f'PRAGMA cache_size = -{cache_size_bytes};'
    ,'.mode ascii'
    ,'.separator "\t" "\n"'
    ,f'.import --skip 1 {tab_delimited_file} {table_name}'
    ,f'ANALYZE {table_name};'
    ,'.quit']

    result = subprocess.run(
        args, text=True, capture_output=True
    )
    
    if result.returncode != 0:
        commands = "\n" + " ".join(args[0:2]) + "\n" + "\n".join(args[2:]) + "\n"
        raise subprocess.CalledProcessError(result.returncode,commands + result.stderr)

    myprint('Data successfully imported!')

#===============================================================================

def get_sql_files():

    """Get our data processing SQL files"""

    cwd = os.getcwd()
    os.chdir('../SQL')
    files = [os.path.abspath(file_name) for file_name in glob.glob('[0-9]_*.sql')]
    files.sort(key=lambda file_name : file_name[0])
    os.chdir(cwd)
    return files

#===============================================================================

def drop_staging_tables(database):

    """Execute file dropping all the staging tables"""

    sql_file = '../SQL/drop_staging_tables.sql'

    execute_sql_file(database,sql_file)

#===============================================================================

def main():

    """Execute our dataset download, data import into staging tables and SQL data processing"""

    # Set the current working directory to the same as this file
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    # Set current working directory to the Data directory, our files will download there
    os.chdir('./Data')

    sqlite_db = 'movies.db'
    url = 'https://datasets.imdbws.com/'
    dataset_urls = get_source_data_urls(url)
    file_and_table_names = generate_file_and_table_names(dataset_urls)

    create_staging_tables(sqlite_db)

    myprint('-' * 20)

    for dataset in file_and_table_names:

        download_dataset(dataset['url'],dataset['zip_file'])
        uncompress_dataset(dataset['zip_file'],dataset['flat_file'])
        myprint(f"Deleting {dataset['zip_file']}")
        os.remove(dataset['zip_file'])
        import_dataset(sqlite_db,dataset['staging_table'],dataset['flat_file'])
        myprint(f"Deleting {dataset['flat_file']}")
        os.remove(dataset['flat_file'])
        myprint('-' * 20)

    for sql_file in get_sql_files():

        execute_sql_file(sqlite_db,sql_file)
        myprint('-' * 20)

    drop_staging_tables(sqlite_db)

#===============================================================================

if __name__ == "__main__":

    main()

