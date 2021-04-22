# exql
Persist Excel/CSV sheets to MySQL (and vice versa)
# Installation
```pip install exql```
# How it works
Convert a directory into a database and the .csv and .xls files in that directory to tables (or vice versa i.e. database/table to director/.csv). Currently, only .csv/.xsl to MySQL conversion is supported.<br>
```
directory <==> database
CSV/Excel sheets in directory <==> tables in database 
```
For example,
```
MyDir           <->     CREATE SCHEMA MyDir;
|-file1.csv     <->     CREATE TABLE file1 (...);
|-file2.xlsx    <->     CREATE TABLE file2 (...);
```
# Setup
### 1. Configure DB Params
In `config.properties`, specify host, port, username and password of database with which to connect.
### 2. Install
Run `pip install .` in root directory
### 3. Import and use
Import using `from exql import exql`
# Usage
| Method  | Description |
| ------------- | ------------- |
| create_db_from_directory  | Create MySQL DB with name same as the directory name and tables built using .csv/.xlsx files present within the directory  |
| create_table_from_csv  | Persist data from a .csv/.xslx file to a single MySQL table |
| insert_in_table|Insert data from .csv into existing table |
|select_into_csv|Select rows read from a DB using the provided query into a .csv|
|delete_from_db|Delete rows matching column-value pairs provided in .csv from MySQL table|
|write_db_to_dir|Create a directory containing all tables of a DB saved as .csv files|
### Note
Refer to the `resources` folder for csv/xsl file templates needed for table creation/insertion/deletion