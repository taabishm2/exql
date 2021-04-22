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
# Usage
Import exql and create an Exql object passing your MySQL username, password, etc.
```python
from exql import exql
exql_obj = exql.Exql("root", "mysql@123", "localhost", 3306, False)
```
Create a database by passing path of directory to be converted to a DB. A sample directory is present in the `resources` directory
```python
exql_obj.create_db_from_directory("/path/demo_university_db")
```
Similarly, other methds can be used to create individual tables, delete rows, etc.

| Method  | Description |
| ------------- | ------------- |
| create_db_from_directory  | Create MySQL DB with name same as the directory name and tables built using .csv/.xlsx files present within the directory  |
| create_table_from_csv  | Persist data from a .csv/.xslx file to a single MySQL table |
| insert_in_table|Insert data from .csv into existing table |
|select_into_csv|Select rows read from a DB using the provided query into a .csv|
|delete_from_db|Delete rows matching column-value pairs provided in .csv from MySQL table|
|write_db_to_dir|Create a directory containing all tables of a DB saved as .csv files|
### Note
- In the .xsl or .csc files, SQL commands can be provided directly. Thus, the .csv or .xsl can have SQL queries as follows

| student_id  | name | age | added_on |
| ------------- | ------------- | ------------- |------------- |
|INT|VARCHAR(100)|INT|DATETIME|
PRIMARY KEY|NOT NULL|NOT NULL|
| | |
100001|(SELECT UPPER('John Keats'))|45|now()
100002|'William Shakespeare'|23|now()
  (SELECT COUNT(*) FROM table2)|'Robert Frost'|54|now()
- Cells containing text must be enclosed in inverted commas or else they would be interpreted as SQL keywords/identifiers
- Refer to the `resources` folder for .csv/.xsl file templates needed for table creation/insertion/deletion