from itertools import repeat

from skyloader.loader_base import LoaderBase
from skyloader.utils import connected

import pandas as pd
import pyodbc


def schema_information(df):
    info_schema = {
        "column": df.columns,
        "dtype": df.dtypes.astype(str),
        "non-null count": df.count(),
    }
    return pd.DataFrame(info_schema).to_dict(orient="list")


class PostgresLoader(LoaderBase):
    def __init__(self, server, database, port=5432, schemaname=None):
        self.server = server
        self.port = port
        self.database = database
        self.connection = None
        self.connected = False
        self.schema = schemaname

    @property
    def schemaname(self):
        return self.schema

    def connect(self):
        self.connection = pyodbc.connect(self.connection_string)
        self.connection.autocommit = True
        self.connected = True

    def create_jdbc_url(self):
        pg_jdbc_url = f"jdbc:postgresql://{server}:{port}/{database}"
        return pg_jdbc_url

    @connected
    def load_datafile(self, datafile):
        self.create_schema_if_not_exists()
        self.create_table_if_not_exists(datafile)
        self.load_data(datafile)

    def write_sql(self):
        (
            df.write.format("jdbc")
            .option("user", pg_user)
            .option("password", pg_pass)
            .option("url", pg_jdbc_url)
            .option("dbtable", target_table)
            .mode(write_mode)
            .save()
        )

    def create_schema_if_not_exists(self):
        ddl = f"""IF NOT EXISTS (SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name = '{self.schemaname}')
        BEGIN
            EXEC('CREATE SCHEMA [{self.schemaname}]')
        END
        """
        logger.info(
            f"Creating schema {self.schemaname} if it does not already exist:\n\n{ddl}"
        )
        self.connection.execute(ddl)
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
        columns_spec = ", ".join(
            f"[{column}] {dtypes_mapping[dtype]}"
            for column, dtype in zip(schema_info["column"], schema_info["dtype"])
        )

        return f"""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{datafile.tablename}' AND TABLE_SCHEMA = N'{self.schemaname}')
        BEGIN
            CREATE TABLE {self.schema}.{datafile.tablename} ({columns_spec})
        END
        """

    def create_table_if_not_exists(self, datafile):
        ddl = self.create_table_statement(datafile)
        logger.info(
            f"Table {self.schema}.{datafile.tablename} does not exist, executing SQL:\n\n{ddl}"
        )
        self.connection.execute(ddl)
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
