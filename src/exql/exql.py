from exql.dao import *
import csv
import xlrd
from exql.logger import logger

from pathlib import Path


def validate_get_csv_fields_for_table_create(file, min_rows):
    """
    Check and return rows of csv as list of tuples
    :param file: .csv file
    :param min_rows: Minimum number of rows the csv must contain
    :return: True if validated, otherwise throw exception
    """
    if not file.is_file() or file.suffix != ".csv":
        raise Exception("The provided path " + file + " does not point to a .csv file")

    csv_file = open(file, "r")
    row_list = list(csv.reader(csv_file))

    if len(row_list) < min_rows:
        raise Exception(str(file) + " does not possess the required csv structure. Refer to the sample files")

    return row_list


def validate_get_xls_fields_for_table_create(file, min_rows):
    """
    Check and return rows of csv as list of tuples
    :param file: .xls file
    :param min_rows: Minimum number of rows the .xls must contain
    :return: True if validated, otherwise throw exception
    """
    if not file.is_file() or file.suffix != ".xls":
        raise Exception("The provided path " + file + " does not point to a .xls file")

    workbook = xlrd.open_workbook(file)
    sheet = workbook.sheet_by_index(0)

    row_list = [sheet.row_values(i) for i in range(sheet.nrows)]

    if len(row_list) < min_rows:
        raise Exception(str(file) + " does not possess the required file structure. Refer to the sample files")

    return row_list


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


def validate_get_rows(file_path, min_rows):
    """
    Read .csv or .xls as list of rows
    :param file_path: Path to .csv or .xls to be read
    :param min_rows: Minimum rows needed in the filw
    :return: List of rows
    """
    if file_path.suffix == ".csv":
        return validate_get_csv_fields_for_table_create(file_path, min_rows)

    if file_path.suffix == ".xls":
        return validate_get_xls_fields_for_table_create(file_path, min_rows)


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
        raise Exception(str(destination_dir_path) + " is not a valid directory")

    destination_dir_path = write_dir_path / destination_file_name
    write_dir_path = Path(destination_dir_path)
    if write_dir_path.exists() or write_dir_path.suffix != ".csv":
        raise Exception(str(write_dir_path) + " must refer to a valid, non-existing CSV file")

    write_file = open(write_dir_path, mode='w')
    csv_writer = csv.writer(write_file)
    csv_writer.writerow(column_names)
    csv_writer.writerows(row_list)
    write_file.close()

    logger.info("Wrote table to " + str(destination_dir_path))


def get_select_all_query(table_name):
    """
    Returns a SELECT all query for the provided :param table_name
    :param table_name: Name of table
    :return: The string "SELECT * FROM :param table_name"
    """
    return "SELECT * FROM " + str(table_name)


class Exql:
    username = password = host = port = None
    strict_structure = False

    def __init__(self, username, password, host, port=3306, strict_stucture=False):
        """
        Initialize parameters to be used in connecting with MySQL DB
        :param username: MySQL username
        :param password: MySQL password
        :param host: MySQL host
        :param port: MySQL port number. If not provided, uses :param port=3306
        :param strict_stucture: This flag determines whether a strict checking is applied in different process. When
        turned on, directories to be converted to DBs must have only .csv/.xls file and no directories. If false, any
        children directories and/or non .csv or non .xls files are ignored
        """
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.strict_structure = strict_stucture

    def validate_and_get_data(self, base_dir):
        """
        Validate is the :param base_dir has the proper structure and returns the list of .csv/.xls files contained
        :param base_dir: Path object referencing the directory specified
        :return: List of .csv/.xls files within the directory
        """
        if not base_dir.exists() or not base_dir.is_dir():
            raise Exception("The path must point to a valid, existing directory")

        child_dirs = [e for e in base_dir.iterdir() if e.is_dir()]

        if self.strict_structure and child_dirs:
            raise Exception("No directories should be present inside the specified directory")

        return self.get_file_map(base_dir)

    def get_file_map(self, base_dir):
        """
        Validate and get map of .csv/.xls file name to list of rows within the csv
        :param base_dir: Base directory where csvs are stored
        :return: Map from file name to list of rows in file
        """
        files = [e for e in base_dir.iterdir() if e.is_file()]
        file_map = dict()

        if not files:
            raise Exception("No .csv/.xls files are present in the specified directory")

        for file in files:
            if self.strict_structure and (file.suffix != ".csv" and file.suffix != ".xls"):
                raise Exception("Files other than .csv or .xls files cannot be present in the directory")

            file_map[file.stem] = validate_get_rows(file, 3)

        return file_map

    def open_cursor_and_connection(self):
        return open_cursor_connection(self.host, self.username, self.password, self.port)

    def create_db_from_directory(self, directory_path):
        """
        Create a Schema based on a directory specified. All valid .csv/.xsls within the directory are converted into tables.
        If any data is present in the .csv/.xls, the rows are also populated
        :return: None
        """
        base_dir = Path(directory_path)
        input_file_map = self.validate_and_get_data(base_dir)

        connection, cursor = self.open_cursor_and_connection()
        create_database(cursor, base_dir.name)
        for file_name in input_file_map:
            file_content = input_file_map[file_name]
            create_table(cursor, base_dir.name, file_name, extract_table_create_data(file_content))

            data_rows = extract_table_data(file_content, 4)
            if data_rows:
                insert_rows(cursor, connection, base_dir.name, file_name, extract_column_names(file_content),
                            data_rows)

        close_cursor_connection(cursor, connection)

        logger.info("Created database " + str(base_dir.name) + " with " + str(len(input_file_map)) + " tables")

    def create_table_from_csv(self, db_name, source_file_path):
        """
        Create a table from the specified .csv/.xls file with name same as .csv/.xls file name
        :return: None
        """
        base_dir = Path(source_file_path)
        source_file_data = validate_get_rows(base_dir, 3)

        connection, cursor = self.open_cursor_and_connection()
        create_table(cursor, db_name, base_dir.stem, extract_table_create_data(source_file_data))

        data_rows = extract_table_data(source_file_data, 4)
        if data_rows:
            insert_rows(cursor, connection, db_name, base_dir.stem, extract_column_names(source_file_data),
                        data_rows)

        close_cursor_connection(cursor, connection)

        logger.info("Created table " + str(base_dir.stem) + "in DB " + db_name)

    def insert_in_table(self, db_name, csv_file_path, table_name=None):
        """
        Insert into existing table with name :param table_name. If table_name not passed, used csv file name as table name
        :param db_name: Name of database
        :param csv_file_path: Path of csv file containing rows to persists. First row must have column names
        :param table_name: Name of table in which to persist. If not provided, use name of csv file
        :return: None
        """
        base_dir = Path(csv_file_path)
        csv_file_data = validate_get_rows(base_dir, 2)

        if not table_name:
            table_name = base_dir.stem

        data_rows = extract_table_data(csv_file_data, 1)

        connection, cursor = self.open_cursor_and_connection()
        insert_rows(cursor, connection, db_name, table_name, extract_column_names(csv_file_data), data_rows)

        logger.info("Inserted " + str(cursor.rowcount) + " rows in " + str(table_name))

        close_cursor_connection(cursor, connection)

    def select_into_csv(self, db_name, full_select_query, destination_dir_path, destination_file_name):
        """
        Select rows read from a DB using the provided query into a csv
        :param db_name: Name of database
        :param full_select_query: Syntactically correct SQL query to run e.g. "SELECT * FROM myTable LIMIT 10;"
        :param destination_dir_path: Path of valid, existing directory where csv is to be stored
        :param destination_file_name: Name with with csv is to be saved (including extension) e.g. "results.csv".
        No file with a similar should exist in the destination directory
        :return:  None
        """
        connection, cursor = self.open_cursor_and_connection()
        select_rows(cursor, db_name, full_select_query)

        row_list = cursor.fetchall()
        column_names = cursor.column_names

        close_cursor_connection(cursor, connection)

        write_to_new_csv(column_names, destination_dir_path, destination_file_name, row_list)

    def delete_from_db(self, db_name, deletion_csv, table_name=None):
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
        csv_file_data = validate_get_rows(base_dir, 2)

        if not table_name:
            table_name = base_dir.stem

        data_rows = extract_table_data(csv_file_data, 1)

        connection, cursor = self.open_cursor_and_connection()
        delete_rows(cursor, connection, db_name, table_name, extract_column_names(csv_file_data), data_rows)

        logger.info("Deleted " + str(cursor.rowcount) + " rows from " + str(table_name))

        close_cursor_connection(cursor, connection)

    def write_db_to_dir(self, destination_path, db_name, table_list=None):
        """
        Write a DB to a directory at the specified :param destination_path
        :param destination_path: Path where directory representing the DB must be stored
        :param db_name: Name of DB to be written
        :param table_list: Optional list of tables to write as csv. If not provided, writes all tables present
        :return: None
        """
        base_dir = Path(destination_path)

        destination_dir_path = base_dir / db_name
        destination_dir_path.mkdir(parents=True, exist_ok=False)

        if table_list:
            for table_name in table_list:
                self.select_into_csv(db_name, get_select_all_query(table_name),
                                     destination_dir_path.absolute(), table_name)
        else:
            connection, cursor = self.open_cursor_and_connection()
            get_all_table_names(cursor, db_name)
            table_name_list = cursor.fetchall()

            for table_name in table_name_list:
                self.select_into_csv(db_name, get_select_all_query(table_name[0]), destination_dir_path.absolute(),
                                     table_name[0] + ".csv")

        logger.info("Wrote DB to directory " + str(destination_dir_path))


if __name__ == '__main__':
    # e = Exql("root", "mysql@123", "localhost", 3306, False)
    #
    # e.create_db_from_directory("/home/user/Desktop/demo/demo_university_db")
    #
    # e.create_table_from_csv("demo_university_db", "../resources/demo_university_db/other/courses.csv")
    #
    # e.insert_in_table("demo_university_db", "../resources/demo_university_db/other/insert_student.csv", "student")
    #
    # e.select_into_csv("demo_university_db", "SELECT * FROM student LIMIT 3;",
    #                 "../resources/demo_university_db/output", "student_result.csv")
    #
    # e.delete_from_db("demo_university_db", "../resources/demo_university_db/other/delete_student.csv", "student")
    #
    # e.write_db_to_dir("../resources/demo_university_db/output", "demo_university_db")
    pass
