from simpleloader.utils import build_connection_string

import configparser
import datetime
import io
import json
import logging

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)

RUN_ID = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

formatter = logging.Formatter(f"%(asctime)s - %(levelname)s - {RUN_ID} - %(message)s")

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

log_stream = io.StringIO()

stream_handler = logging.StreamHandler(log_stream)
stream_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(stream_handler)


try:
    from databricks.sdk.runtime import dbutils

    connection_string_params = {
        "driver": "{ODBC Driver 18 for SQL Server}",
        "server": dbutils.widgets.get("server"),
        "database": dbutils.widgets.get("database"),
        "port": dbutils.widgets.get("port"),
        "extra_connection_params": dbutils.widgets.get("extra_db_connection_params"),
        "uid": dbutils.secrets.get("fpna", "finsql-user"),
        "pwd": dbutils.secrets.get("fpna", "finsql-pass"),
    }
    SQL_SERVER_SCHEMA = dbutils.widgets.get("sql_server_schema")
    SQL_SERVER_CONNECTION_STRING = build_connection_string(**connection_string_params)
    GDRIVE_ROOT_FOLDER_ID = dbutils.widgets.get("gdrive_root_folder_id")
    GDRIVE_SA_TOKEN_STRING = dbutils.secrets.get("bumapper", "gdrive_sa_token_string")
    LOCAL_FILE_PATH = ""
    IN_DATABRICKS = True
except ModuleNotFoundError:
    config = configparser.ConfigParser(interpolation=None)
    config.read("config.ini")
    connection_string_params = {
        "driver": "{ODBC Driver 18 for SQL Server}",
        "server": config["config"]["sql_server_host"],
        "database": config["config"]["sql_server_db"],
        "port": config["config"]["sql_server_port"],
        "extra_connection_params": config["config"]["sql_server_extra"],
        "uid": config["config"]["sql_server_uid"],
        "pwd": config["config"]["sql_server_pwd"],
    }
    SQL_SERVER_SCHEMA = config["config"]["sql_server_schema"]
    GDRIVE_SA_TOKEN_STRING = config["config"]["gdrive_sa_token_string"]
    SQL_SERVER_CONNECTION_STRING = build_connection_string(**connection_string_params)
    GDRIVE_ROOT_FOLDER_ID = config["config"]["gdrive_root_folder_id"]
    IN_DATABRICKS = False

GDRIVE_SA_TOKEN = json.loads(GDRIVE_SA_TOKEN_STRING)
DEBUG_PARAMS = {
    "in_databricks": IN_DATABRICKS,
    "gdrive_root_folder_id": GDRIVE_ROOT_FOLDER_ID,
    "sql_server_schema": SQL_SERVER_SCHEMA,
    **{k:connection_string_params[k] for k in connection_string_params if k not in ("uid", "pwd")}
}

logger.info(
    f"Starting up with parameters {DEBUG_PARAMS=}"
)
