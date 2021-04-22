class MySql:
    DELETE_ROWS = "DELETE FROM {table_name} WHERE {row_values}"
    SELECT_ROWS = "{select_query}"
    INSERT_ROWS = "INSERT INTO {table_name}({column_names}) VALUES {row_values};"
    USE_DB = "USE {db_name};"
    CREATE_TABLE = "CREATE TABLE {table_name} ({row_data_list});"
    CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS {db_name};"
