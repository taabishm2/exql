import dao
import csv
from logger import logger
from pathlib import Path

# TODO:Get from props
strict_structure = False


def validate_get_fields_for_table_create(file, min_rows):
    """
    Check and return rows of csv as list of tuples
    :param file: .csv file
    :param min_rows: Minimum number of rows the csv must contain
    :return: True if validated, otherwise throw exception
    """
    if not file.is_file() or file.suffix != ".csv":
        raise Exception("The provided path " + file + " does not point to a csv file")

    csv_file = open(file, "r")
    row_list = list(csv.reader(csv_file))

    if len(row_list) < min_rows:
        raise Exception(str(file) + " does not possess the required csv structure. Refer to the sample csvs")

    return row_list


def validate_and_get_csvs(base_dir):
    """
    Validate is the :param base_dir has the proper structure and returns the list of csv files contained
    :param base_dir: Path object referencing the directory specified
    :return: List of csv files within the directory
    """
    if not base_dir.exists() or not base_dir.is_dir():
        raise Exception("The path must point to a valid, existing directory")

    child_dirs = [e for e in base_dir.iterdir() if e.is_dir()]
    if strict_structure and child_dirs:
        raise Exception("No directories should be present inside the specified directory")

    return get_file_map(base_dir)


def get_file_map(base_dir):
    """
    Validate and get map of csv file name to list of rows within the csv
    :param base_dir: Base directory where csvs are stored
    :return: Map from file name to list of rows in file
    """
    files = [e for e in base_dir.iterdir() if e.is_file()]
    file_map = dict()

    if not files:
        raise Exception("No csv files are present in the specified directory")

    for file in files:
        if strict_structure and file.suffix != ".csv":
            raise Exception("Files with extensions other than .csv cannot be present in specified directory")

        if file.suffix == ".csv":
            file_map[file.stem] = validate_get_fields_for_table_create(file, 3)

    return file_map


def open_cursor_and_connection():
    # TODO: Read from props
    return dao.open_cursor_connection("localhost", "root", "mysql@123")


def extract_table_create_data(row_list):
    """
    Extract the column information i.e. name, type and modifiers (if any) from first 3 rows of the :param row_list
    :param row_list: List of all rows within a csv used during table creation, including header and body rows
    :return: Table creation data map with keys "name", "type" and "modifiers"
    """
    column_creation_maps = []
    column_data_zip = list(zip(row_list[0], row_list[1], row_list[2]))

    for data in column_data_zip:
        column_creation_maps.append(dict({
            "name": data[0],
            "type": data[1],
            "modifiers": data[2]
        }))

    return column_creation_maps


def extract_table_data(row_list, n):
    """
    Extract table tows from all row as a list of tuples for persisting.
    First :param n rows of csv are metadata, header rows. Thus, returns rows n and onwards
    :param row_list: List of all rows in csv used for table creation
    :param n: Represents how any rows in the start are header rows. Returns [n:]
    :return: Extracted list of tuples representing rows to be persisted
    """
    return row_list[n:]


def extract_column_names(row_list):
    """
    Extract names of columns from row list obtained from table csv. The first row contains all row names
    :param row_list: List of all rows in csv used for table creation
    :return: List of names present in table csv
    """
    return row_list[0]


def create_db_from_directory(directory_path="/home/user/Desktop/exql/exql/test_dir"):
    """
    Create a Schema based on a directory specified. All valid csvs within the directory are converted into tables.
    If any data is present in the csv, the rows are also populated
    :return: None
    """
    base_dir = Path(directory_path)
    csv_file_map = validate_and_get_csvs(base_dir)

    connection, cursor = open_cursor_and_connection()
    dao.create_database(cursor, base_dir.name)
    for file_name in csv_file_map:
        file_content = csv_file_map[file_name]
        dao.create_table(cursor, base_dir.name, file_name, extract_table_create_data(file_content))

        data_rows = extract_table_data(file_content, 4)
        if data_rows:
            dao.insert_rows(cursor, connection, base_dir.name, file_name, extract_column_names(file_content), data_rows)

    dao.close_cursor_connection(cursor, connection)

def create_table_from_csv(db_name, csv_file_path="/home/user/Desktop/exql/exql/file3.csv"):
    """
    Create a Table from the specified csv file with name same as csv file name
    :return: None
    """
    base_dir = Path(csv_file_path)
    csv_file_data = validate_get_fields_for_table_create(base_dir, 3)

    connection, cursor = open_cursor_and_connection()
    dao.create_table(cursor, db_name, base_dir.stem, extract_table_create_data(csv_file_data))

    data_rows = extract_table_data(csv_file_data, 4)
    if data_rows:
        dao.insert_rows(cursor, connection, db_name, base_dir.stem, extract_column_names(csv_file_data), data_rows)

    dao.close_cursor_connection(cursor, connection)


def insert_in_table(db_name, csv_file_path="/home/user/Desktop/exql/exql/test_dir/file1.csv", table_name=None):
    """
    Insert into existing table with name :param table_name. If table_name not passed, used csv file name as table name
    :param db_name: Name of database
    :param csv_file_path: Path of csv file containing rows to persists. First row must have column names
    :param table_name: Name of table in which to persist. If not provided, use name of csv file
    :return: None
    """
    base_dir = Path(csv_file_path)
    csv_file_data = validate_get_fields_for_table_create(base_dir, 2)

    if not table_name:
        table_name = base_dir.stem

    data_rows = extract_table_data(csv_file_data, 1)

    connection, cursor = open_cursor_and_connection()
    dao.insert_rows(cursor, connection, db_name, table_name, extract_column_names(csv_file_data), data_rows)

    dao.close_cursor_connection(cursor, connection)


def select_into_csv(db_name, full_select_query, destination_dir_path, destination_file_name):
    """
    Select rows read from a DB using the provided query into a csv
    :param db_name: Name of database
    :param full_select_query: Syntactically correct SQL query to run e.g. "SELECT * FROM myTable LIMIT 10;"
    :param destination_dir_path: Path of valid, existing directory where csv is to be stored
    :param destination_file_name: Name with with csv is to be saved (including extension) e.g. "results.csv".
    No file with a similar should exist in the destination directory
    :return:  None
    """
    connection, cursor = open_cursor_and_connection()
    dao.select_rows(cursor, db_name, full_select_query)

    row_list = cursor.fetchall()
    column_names = cursor.column_names

    dao.close_cursor_connection(cursor, connection)

    write_to_new_csv(column_names, destination_dir_path, destination_file_name, row_list)


def write_to_new_csv(column_names, destination_dir_path, destination_file_name, row_list):
    """
    Write list of rows fetched from the database into a new csv file at the specified location
    :param column_names: List of column names for the table
    :param destination_dir_path: Path of directory where csv should be saved
    :param destination_file_name: Name to give the file when saving (including extension) e.g. "student_data.csv"
    :param row_list: List of rows fetched from the database
    :return: None
    """
    write_dir_path = Path(destination_dir_path)
    if not write_dir_path.is_dir():
        raise Exception(destination_dir_path + " is not a valid directory")

    destination_dir_path = write_dir_path / destination_file_name
    write_dir_path = Path(destination_dir_path)
    if write_dir_path.exists() or write_dir_path.suffix != ".csv":
        raise Exception(destination_file_name + " must refer to a valid, non-existing CSV file")

    write_file = open(write_dir_path, mode='w')
    csv_writer = csv.writer(write_file)
    csv_writer.writerow(column_names)
    csv_writer.writerows(row_list)
    write_file.close()


def delete_from_db(db_name, deletion_csv, table_name=None):
    """
    Delete all rows matching conditions determined by the specified :param deletion_csv. :param deletion_csv must
    contain column names in the first row and value in subsequent rows. Deletion query is created as follows:
    "DELETE FROM :param table_name WHERE (col1=valA AND col2=valB) OR (col1=valX AND col2=valY) where col1, col2 are
    column names and (valA,valB) and (valX,valY) are two rows in the csv
    :param db_name: Name of database
    :param deletion_csv: CSV containing column names and values to be used for deletion
    :param table_name: Name of table from which to delete. If not provided, uses name of csv file
    :return: None
    """
    base_dir = Path(deletion_csv)
    csv_file_data = validate_get_fields_for_table_create(base_dir, 2)

    if not table_name:
        table_name = base_dir.stem

    data_rows = extract_table_data(csv_file_data, 1)

    connection, cursor = open_cursor_and_connection()
    dao.delete_rows(cursor, connection, db_name, table_name, extract_column_names(csv_file_data), data_rows)

    dao.close_cursor_connection(cursor, connection)


if __name__ == '__main__':
    #create_db_from_directory()
    #create_table_from_csv("test_dir")
    #insert_in_table("test_dir", "/home/user/Desktop/exql/exql/test_dir/file3.csv", "file1")
    #select_into_csv("test_dir", "SELECT * FROM file1 LIMIT 2;", "/home/user/Desktop/exql/exql/test_dir", "result.csv")
    delete_from_db("test_dir","/home/user/Desktop/exql/exql/test_dir/file3.csv", "file1")