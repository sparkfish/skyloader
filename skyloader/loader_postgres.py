import logging
from itertools import repeat

from skyloader.loader_base import LoaderBase
from skyloader.utils import connected

import pandas as pd
import psycopg2
from psycopg2 import sql

logger = logging.getLogger(__name__)


def schema_information(df):
    info_schema = {
        "column": df.columns,
        "dtype": df.dtypes.astype(str),
        "non-null count": df.count(),
    }
    return pd.DataFrame(info_schema).to_dict(orient="list")


class PostgresLoader(LoaderBase):
    def __init__(self, server, database, port=5432, schemaname=None, user=None, password=None):
        self.server = server
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.connected = False
        self.schema = schemaname

    @property
    def schemaname(self):
        return self.schema

    def connect(self):
        self.connection = psycopg2.connect(
            host=self.server,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
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
        # Use parameterized query to prevent SQL injection
        ddl = "CREATE SCHEMA IF NOT EXISTS %s"
        logger.info(
            f"Creating schema {self.schemaname} if it does not already exist"
        )
        cursor = self.connection.cursor()
        cursor.execute(sql.SQL(ddl).format(sql.Identifier(self.schemaname)))
        cursor.close()
        logger.info(f"DDL SQL for schema {self.schemaname} finished successfully")

    def create_table_statement(self, datafile):
        tablename = datafile.tablename
        schema_info = schema_information(datafile.data)
        dtypes_mapping = {
            "object": "TEXT",
            "float64": "DOUBLE PRECISION",
            "int64": "BIGINT",
            "datetime64[ns]": "TIMESTAMP",
            "bool": "BOOLEAN",
            "timedelta[ns]": "INTERVAL",
            "category": "TEXT",
        }
        
        # Validate column names to prevent injection
        safe_columns = []
        for column, dtype in zip(schema_info["column"], schema_info["dtype"]):
            # Basic validation - only allow alphanumeric and underscore
            if not column.replace('_', '').replace(' ', '').isalnum():
                raise ValueError(f"Invalid column name: {column}")
            if dtype not in dtypes_mapping:
                raise ValueError(f"Unsupported data type: {dtype}")
            safe_columns.append(f'"{column}" {dtypes_mapping[dtype]}')
        
        columns_spec = ", ".join(safe_columns)

        return f"""
        CREATE TABLE IF NOT EXISTS "{self.schema}"."{datafile.tablename}" ({columns_spec})
        """

    def create_table_if_not_exists(self, datafile):
        ddl = self.create_table_statement(datafile)
        logger.info(
            f"Creating table {self.schema}.{datafile.tablename} if it does not exist"
        )
        cursor = self.connection.cursor()
        cursor.execute(ddl)
        cursor.close()
        logger.info(f"DDL SQL executed successfully")

    def perform_load(self, datafile):
        insert = self.insert_statement(datafile.data.columns, datafile.tablename)
        logger.debug(f"Executing SQL: \n{insert}")
        cursor = self.connection.cursor()
        logger.info(f"{datafile.records}")
        cursor.executemany(insert, list(datafile.records))
        cursor.close()

    def insert_statement(self, column_names, tablename):
        columns = ", ".join(f'"{col}"' for col in column_names)
        placeholders = ",".join(repeat("%s", len(column_names)))
        return f'INSERT INTO "{self.schemaname}"."{tablename}" ({columns}) VALUES ({placeholders})'

    def load_data(self, datafile):
        # We don't want to autocommit here, since the driver will issue a commit for each record in the load
        # Rather we'll commit explicitly at the end so that either all the records are committed, or none are
        self.connection.autocommit = False
        try:
            logger.info(f"Loading records from {datafile} into PostgreSQL")
            self.perform_load(datafile)
            self.connection.commit()
        except psycopg2.DatabaseError as err:
            self.connection.rollback()
            logger.error(f"Loading of data into PostgreSQL of {datafile} failed")
            raise err
        else:
            logger.info(
                f"Load successful. Loaded {datafile.processed} records from {datafile} into PostgreSQL"
            )
        finally:
            self.connection.autocommit = True
