# IMDB Movie Data Pipeline
A Python 3 and SQL data pipeline example that showcases retrieving and importing [imdb.com](https://www.imdb.com/) datasets and building a queryable relational data model. 

## How to use
- Have Python >= 3.9 installed on your machine
- Clone or download this repository
- In a shell, execute the `movie-data-pipeline.py` script with Python 3

## Contents
- __movie-data-pipeline.py__: Python 3 script managing our data pipeline
- __imdb-datasets-interface.md__: Dataset interface document provided by IMDB, reproduced here in markdown for convenient viewing. Explains dataset contents and field types/formats.
- __data-model.png__: Image file showing the final relational data model
- __README.md__: The document you are currently reading
- __Data__: Source data downloaded and processed within this folder (files are deleted as processed)
  - __movies.db__: The target SQLite database is created here
  - __output.txt__: A sample of output produced as the pipeline is executing
- __SQL__: Files creating the schema and data transformations
  - __create_staging_tables.sql__: _DDL_ to create the staging tables for dataset imports
  - __1_title_basics_data_processing.sql__: _SQL_ statements to process data imported into table 'STAGING_TITLE_BASICS'. Normalizes data (where required) into dimensional and fact tables.
  - __2_name_basics_data_processing.sql__: processing 'STAGING_NAME_BASICS' data (see 1_*.sql)
  - __3_title_principals_data_processing.sql__: processing 'STAGING_TITLE_PRINCIPALS' data (see 1_*.sql)
  - __4_title_episode_data_processing.sql__: processing 'STAGING_TITLE_EPISODE' data (see 1_*.sql)
  - __5_title_ratings_data_processing.sql__: processing 'STAGING_TITLE_RATINGS' data (see 1_*.sql)
  - __6_title_crew_data_processing.sql__: processing 'STAGING_TITLE_CREW' data (see 1_*.sql)
  - __7_title_akas_data_processing.sql__: processing 'STAGING_TITLE_AKAS' data (see 1_*.sql)
  - __drop_staging_tables.sql__: _DDL_ to drop the staging tables when data import and transformation completed 

## Background
This project is designed to display proficiency with two common technologies used in data handling occupations (engineers, analysts etc.), namely Python and SQL. The project also demonstrates knowledge of data cleansing, data normalization, data modeling, and database design.

[imdb.com](https://www.imdb.com/) kindly provides a subset of their data for personal
and non-commercial use. 

Here is an overview of how the data pipeline operates:

- Get the dataset urls from <https://datasets.imdbws.com/>
- Create staging tables in database
- Loop over dataset urls:
  - Download and write compressed (*.gz) dataset to disk, use chunks to limit memory usage as some files are large
  - Uncompress dataset and write flat file (*.tsv) to disk
  - Delete compressed (*.gz) dataset
  - Import uncompressed (*.tsv) dataset into database staging table
  - Delete uncompressed (*.tsv) dataset
- Loop over our enumerated SQL files:
  - Clean, organize and normalize each staging table into its final form/s
- Drop staging tables from database, all our data is now in dimensional and fact tables ready for further analysis

### How is the data?
- Alphanumeric row ID values used where integer _should_ work better (relational databases are usually optimized for integer operations) 
- Weird control characters present in some text fields need replacing
- Delimited values in text fields require splitting out into rows
- Duplicate rows found. IMDB forced uniqueness with 'ordering' field where duplicate values were present?
- Missing values denoted by the characters '\N' (backslash, uppercase n) need replacing with actual NULL

### To do
The data model is queryable in its current form but 'Person' related data is split across multiple source datasets and final tables. Some information is represented in multiple places, i.e. Writers and Directors are present in both our 'Crew' and 'Principal' tables. This obviously 'smells' somewhat and will need addressing at a future date.  
