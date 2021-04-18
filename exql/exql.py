import dao
import csv
from logger import logger
from pathlib import Path

# TODO:Get from props
strict_structure = False


def validate_get_fields(file):
    """
    Check if the provided csv file matches the structure required for creation
    :param file: .csv file
    :return: True if validated, otherwise throw exception
    """
    csv_file = open(file, "r")
    row_list = list(csv.reader(csv_file))

    if len(row_list) < 3:
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
        if strict_structure and file.suffix != "csv":
            raise Exception("Files with extensions other than .csv cannot be present in specified directory")

        if file.suffix == ".csv":
            file_map[file.stem] = validate_get_fields(file)

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


def extract_table_data(row_list):
    """
    Extract table tows from all row as a list of tuples for persisting.
    First 4 rows of csv used for table creation are header rows. Thus, returns rows 4 and onwards
    :param row_list: List of all rows in csv used for table creation
    :return: Extracted list of tuples representing rows to be persisted
    """
    return row_list[4:]


def extract_column_names(row_list):
    """
    Extract names of columns from row list obtained from table creation csv. The first row contains all row names
    :param row_list: List of all rows in csv used for table creation
    :return: List of names present in table creation csv
    """
    return row_list[0]


def create_db_from_directory(directory_path="/home/user/Desktop/exql/exql/test_dir"):
    """
    Create a Schema based on a directory specified. All valid csvs within the directory are converted into tables.
    If any data is present in the csv, the rows are also populated
    :return: none
    """
    base_dir = Path(directory_path)

    csv_file_map = validate_and_get_csvs(base_dir)

    connection, cursor = open_cursor_and_connection()
    dao.create_database(cursor, base_dir.name)
    for file_name in csv_file_map:
        file_content = csv_file_map[file_name]
        dao.create_table(cursor, base_dir.name, file_name, extract_table_create_data(file_content))

        data_rows = extract_table_data(file_content)
        if data_rows:
            dao.insert_rows(cursor, connection, base_dir.name, file_name, extract_column_names(file_content), data_rows)


if __name__ == '__main__':
    create_db_from_directory()
