import logging
from itertools import repeat

from skyloader.utils import connected
from skyloader.loader_base import LoaderBase

import pandas as pd
import pyodbc

logger = logging.getLogger(__name__)


def schema_information(df):
    info_schema = {
        "column": df.columns,
        "dtype": df.dtypes.astype(str),
        "non-null count": df.count(),
    }
    return pd.DataFrame(info_schema).to_dict(orient="list")


class SqlLoader(LoaderBase):
    def __init__(self, server=None, database=None, user=None, password=None, port=1433, schemaname=None, connection_string=None):
        # Allow either connection_string OR individual parameters
        if connection_string:
            if not isinstance(connection_string, str) or not connection_string.strip():
                raise ValueError("Connection string must be a non-empty string")
            self.connection_string = connection_string.strip()
        else:
            # Validate required parameters when not using connection string
            if not server or not isinstance(server, str):
                raise ValueError("Server must be a non-empty string")
            if not database or not isinstance(database, str):
                raise ValueError("Database must be a non-empty string")
            if not user or not isinstance(user, str):
                raise ValueError("User must be a non-empty string")
            if not password or not isinstance(password, str):
                raise ValueError("Password must be a non-empty string")
            
            # Validate port
            if not isinstance(port, int) or port <= 0 or port > 65535:
                raise ValueError("Port must be an integer between 1 and 65535")
            
            # Validate server name format (basic check for suspicious characters)
            if any(char in server for char in [';', '--', '/*', '*/', 'xp_', 'sp_']):
                raise ValueError("Server name contains suspicious characters")
            
            # Validate database name format
            if any(char in database for char in [';', '--', '/*', '*/', 'DROP', 'DELETE', 'INSERT', 'UPDATE']):
                raise ValueError("Database name contains suspicious characters or SQL keywords")
            
            # Build connection string from parameters
            from skyloader.utils import build_connection_string
            self.connection_string = build_connection_string(
                driver="{ODBC Driver 17 for SQL Server}",
                server=server.strip(),
                database=database.strip(),
                uid=user.strip(),
                pwd=password,  # Don't strip password
                port=str(port)
            )
        
        # Validate schema name if provided
        if schemaname is not None:
            if not isinstance(schemaname, str) or not schemaname.strip():
                raise ValueError("Schema name must be a non-empty string if provided")
            # Basic validation for schema name - alphanumeric, underscore, no spaces at start/end
            if not schemaname.replace('_', '').isalnum():
                raise ValueError("Schema name can only contain alphanumeric characters and underscores")
            self.schema = schemaname.strip()
        else:
            self.schema = None
        
        self.connection = None
        self.connected = False

    @property
    def schemaname(self):
        return self.schema

    def connect(self):
        self.connection = pyodbc.connect(self.connection_string)
        self.connection.autocommit = True
        self.connected = True

    def close_connection(self):
        if self.connection:
            self.connection.close()
            self.connected = False

    @connected
    def load_datafile(self, datafile):
        self.create_schema_if_not_exists()
        self.create_table_if_not_exists(datafile)
        self.load_data(datafile)

    def create_schema_if_not_exists(self):
        ddl = """IF NOT EXISTS (SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name = ?)
        BEGIN
            EXEC('CREATE SCHEMA [' + ? + ']')
        END
        """
        logger.info(
            f"Creating schema {self.schemaname} if it does not already exist"
        )
        self.connection.execute(ddl, self.schemaname, self.schemaname)
        logger.info(f"DDL SQL for schema {self.schemaname} finished successfully")

    def create_table_statement(self, datafile):
        tablename = datafile.tablename
        schema_info = schema_information(datafile.data)
        dtypes_mapping = {
            "object": "nvarchar(max)",
            "float64": "float",
            "int64": "bigint",
            "datetime64[ns]": "datetime",
            "bool": "bit",
            "timedelta[ns]": "time",
            "category": "nvarchar(max)",
        }
        
        safe_columns = []
        for column, dtype in zip(schema_info["column"], schema_info["dtype"]):
            # Basic validation - only allow alphanumeric and underscore
            if not column.replace('_', '').replace(' ', '').isalnum():
                raise ValueError(f"Invalid column name: {column}")
            if dtype not in dtypes_mapping:
                raise ValueError(f"Unsupported data type: {dtype}")
            safe_columns.append(f"[{column}] {dtypes_mapping[dtype]}")
        
        columns_spec = ", ".join(safe_columns)

        return f"""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?)
        BEGIN
            CREATE TABLE [{self.schema}].[{datafile.tablename}] ({columns_spec})
        END
        """, [datafile.tablename, self.schemaname]

    def create_table_if_not_exists(self, datafile):
        ddl, params = self.create_table_statement(datafile)
        logger.info(
            f"Table {self.schema}.{datafile.tablename} does not exist, executing SQL"
        )
        self.connection.execute(ddl, params)
        logger.info(f"DDL SQL executed successfully")

    def perform_load(self, datafile):
        insert = self.insert_statement(datafile.data.columns, datafile.tablename)
        logger.debug(f"Executing SQL: \n{insert}")
        cursor = self.connection.cursor()
        cursor.fast_executemany = True
        logger.info(f"{datafile.records}")
        cursor.executemany(insert, datafile.records)
        cursor.commit()
        cursor.close()

    def insert_statement(self, column_names, tablename):
        columns = ", ".join(column_names)
        q = ",".join(repeat("?", len(column_names)))
        return f"INSERT INTO {self.schemaname}.{tablename} ({columns}) VALUES ({q})"

    def load_data(self, datafile):
        # We don't want to autocommit here, since the driver will issue a commit for each record in the load
        # Rather we'll commit explicitly at the end so that either all the records are committed, or none are
        self.autocommit = False
        try:
            logger.info(f"Loading records from {datafile} into SQL Server")
            self.perform_load(datafile)
        except pyodbc.DatabaseError as err:
            self.connection.rollback()
            logger.error(f"Loading of data into SQL Server of {datafile} failed")
            raise err
        else:
            logger.info(
                f"Load successful. Loaded {datafile.processed} records from {datafile} into SQL Server"
            )
            self.autocommit = True
