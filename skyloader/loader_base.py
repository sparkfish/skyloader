from abc import abstractmethod


class LoaderBase():

    def __init__(self, schemaname=None):
        self.schema = schemaname

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close_connection()

    def __del__(self):
        self.close_connection()

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close_connection(self):
        pass

    @abstractmethod
    def load_datafile(self, datafile):
        pass

    @abstractmethod
    def create_schema_if_not_exists(self):
        pass

    @abstractmethod
    def create_table_statement(self, datafile):
        pass

    @abstractmethod
    def create_table_if_not_exists(self, datafile):
        pass

    @abstractmethod
    def perform_load(self, datafile):
        pass

    @abstractmethod
    def insert_statement(self, column_names, tablename):
        pass

    @abstractmethod
    def load_data(self, datafile):
        pass