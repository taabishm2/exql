from exql.logger import logger
import mysql.connector
from exql.sql import MySql


def open_cursor_connection(host, username, password, port):
    """
    Connect to the MySQL DB at the specified host
    :param host: Host address for MySQL database
    :param username: MySQL Database user's username
    :param password: MySQL Database user's password
    :param port: Port for MySQL Database
    :return: Returns the tuple (connection object, cursor object)
    """
    connection = mysql.connector.connect(
        host=host,
        user=username,
        password=password,
        port=port)

    cursor = connection.cursor()
    return connection, cursor


def close_cursor_connection(cursor, connection):
    """
    Closes both the passed cursor and connection
    :param cursor: Cursor to be closed
    :param connection: Connection to be closed
    :return: none
    """
    cursor.close()
    connection.close()
    return


def create_database(cursor, db_name):
    """
    Creates a new schema with the specified name (if not exists)
    :param cursor: SQL connector cursor
    :param db_name: Name of schema to be created
    :return: none
    """
    query = MySql.CREATE_DATABASE
    query = query.format(db_name=db_name)

    logger.info(query)
    cursor.execute(query)


def get_create_table_field_data(field_data):
    """
    Generates the field wise query segments needed to create a table.
    :param field_data: List of dicts with each dict having keys 'name', 'type' and, optionally, 'modifiers'
    :return: none
    """
    field_query_list = []

    for field in field_data:
        query_field_segment = field["name"] + " " + field["type"]
        if field["modifiers"]:
            query_field_segment += " " + field["modifiers"]

        field_query_list.append(query_field_segment)

    return ", ".join(field_query_list)


def create_table(cursor, db_name, table_name, field_data):
    """
    Create a new table in the :param db_name database with name :param table_name. :param field_data must be a list
    of dicts() with each dict containing the 'name', 'type' and 'modifiers' (optional) values for a column to be
    created. E.g. 'name': 'myNewTable', 'type': 'VARCHAR(20)', 'modifiers': 'NOT NULL UNIQUE'
    :param cursor: SQL connector cursor
    :param db_name: Name of databse in which to create table
    :param table_name: Name of table to create. No table with the same name must except
    :param field_data: List of dicts with each dict having keys 'name', 'type' and, optionally, 'modifiers'
    :return: none
    """
    query = MySql.CREATE_TABLE
    query = query.format(table_name=table_name, row_data_list=get_create_table_field_data(field_data))

    logger.info(query)
    cursor.execute(MySql.USE_DB.format(db_name=db_name))
    cursor.execute(query)


def get_insert_rows_field_data(row_data):
    """
    Generates the row wise query segments needed to insert rows into a table.
    :param row_data: Ordered collection of row values
    :return: Row insertion query segements like '('val1', 123, 'val2'), ('valA', 456, 'valB')'
    """
    return ", ".join(
        ["(" + ", ".join(map(str, row)) + ")" for row in row_data]
    )


def insert_rows(cursor, connection, db_name, table_name, column_names, row_data):
    """
    Insert one or more rows in the specified table
    :param cursor: SQL connection cursor
    :param connection: SQL connection object
    :param db_name: Name of database
    :param table_name: Name of table in which to insert rows
    :param column_names: Tuple of columns in which to perform insertion
    :param row_data: List of tuples representing rows. Must match order of :param column_names
    :return: none
    """
    query = MySql.INSERT_ROWS
    query = query.format(table_name=table_name,
                         column_names=", ".join(column_names),
                         row_values=get_insert_rows_field_data(row_data))

    logger.info(query)
    cursor.execute(MySql.USE_DB.format(db_name=db_name))
    cursor.execute(query)
    connection.commit()


def select_rows(cursor, db_name, select_query):
    """
    Select rows using provided :param select_query
    :param cursor: SQL connection cursor
    :param db_name: Name of database
    :param select_query: Full select query to be used for selection e.g "SELECT * FROM myTable LIMIT 100;"
    :return:
    """
    query = MySql.SELECT_ROWS
    query = query.format(select_query=select_query)

    logger.info(query)
    cursor.execute(MySql.USE_DB.format(db_name=db_name))
    cursor.execute(query)


def get_delete_rows_data(column_names, row_deletion_data):
    """
    Returns query segment to delete rows whose :param column_names are equal to any of the tuples
    in :param row_deletion_data
    :param column_names: List of column name to match
    :param row_deletion_data: Values corresponding to the :param column_names to match
    :return: Row deletion query segements like '(col1=val1 AND col2=val2) OR (col1=val3 AND col2=val4)
    """
    query_segment_list = []
    for row in row_deletion_data:
        assert len(column_names) == len(row)

        zipped_list = list(zip(column_names, row))
        zipped_list = [str(item[0]) + "=" + str(item[1]) for item in zipped_list]
        query_segment_list.append("(" + " AND ".join(zipped_list) + ")")

    return " OR ".join(query_segment_list)


def delete_rows(cursor, connection, db_name, table_name, column_names, row_deletion_data):
    """
    Delete rows which match values provided in :param row_deletion_data. Rows whose :param column_name match the
    passed :param row_deletion_data values are deleted
    :param cursor: SQL connection cursoe
    :param connection: SQL connection object
    :param db_name: Name of database
    :param table_name: Name of table from which to delete
    :param column_names: Ordered collection of column_names to use for matching in WHERE query
    :param row_deletion_data: List of tuples, matching the :param column_names ordering, used for equality condition
    during deletion
    :return: none
    """
    query = MySql.DELETE_ROWS
    query = query.format(table_name=table_name,
                         row_values=get_delete_rows_data(column_names, row_deletion_data))

    logger.info(query)
    cursor.execute(MySql.USE_DB.format(db_name=db_name))
    cursor.execute(query)
    connection.commit()


def get_all_table_names(cursor, db_name):
    """
    Returns names of all the tables present in the specified DB
    :param cursor: DB connection cursor
    :param db_name: Name of Database in which to check talbes
    :return: List of tables
    """
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = '{db_name}';"
    query = query.format(db_name=db_name)

    logger.info(query)
    cursor.execute(query)


if __name__ == '__main__':
    # local_connection, local_cursor = open_cursor_connection("localhost", "root", "mysql@123")
    # 
    # create_database(local_cursor, "myTestDb")
    # 
    # field_data_list = [{"name": "student_name", "type": "VARCHAR(20)", "modifiers": "UNIQUE NOT NULL"},
    #                    {"name": "student_roll", "type": "INT", "modifiers": "PRIMARY KEY AUTO_INCREMENT"}]
    # 
    # # create_table(local_cursor, "myTestDb", "myTestTable", field_data_list)
    # 
    # column_name_list = ("student_name", "student_roll")
    # row_data_list = [("\"stua\"", 1253), ("\"stub\"", 5342), ("\"stuc\"", 3856)]
    # 
    # # insert_rows(local_cursor, local_connection, "myTestDb", "myTestTable", column_name_list, row_data_list)
    # 
    # # select_rows(local_cursor, "myTestDb", "select * from myTestTable;")
    # 
    # # print(get_delete_rows_data(["a", "b", "c"], [[1, 2, 3], ["\"x\"", "\"y\"", "\"z\""]]))
    # 
    # delete_rows(local_cursor, local_connection, "myTestDb", "myTestTable", ("student_name",), [("\"stua\"",), ("\"stub\"",)])
    
    pass
