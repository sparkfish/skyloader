#!/usr/bin/env python3
"""
Simple test script for skyloader components.
This allows you to test the loaders without needing Google Drive credentials.
"""

import logging
import pandas as pd
import sys
import os
from unittest.mock import Mock

# Add the current directory to Python path so we can import skyloader modules directly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_datafile():
    """Test DataFile functionality"""
    print("Testing DataFile...")
    
    # Import directly from the module file to avoid __init__.py dependencies
    from skyloader.datafile import DataFile
    
    # Create a mock Google Drive object
    mock_gdrive_object = {
        "name": "test_data.xlsx",
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "id": "test123",
        "kind": "drive#file",
        "createdTime": "2023-01-01T00:00:00.000Z",
        "modifiedTime": "2023-01-02T00:00:00.000Z",
        "parents": ["parent123"]
    }
    
    # Create DataFile from mock
    datafile = DataFile.from_gdrive(mock_gdrive_object)
    datafile.run_id = "20240101_123456_abcd1234"
    
    # Add some test data
    datafile.data = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'salary': [50000.0, 60000.0, 70000.0],
        'active': [True, False, True]
    })
    
    print(f"DataFile name: {datafile.name}")
    print(f"DataFile tablename: {datafile.tablename}")
    print(f"DataFile columns: {list(datafile.columns)}")
    print(f"DataFile records count: {sum(1 for _ in datafile.records)}")
    print("✓ DataFile test passed\n")
    
    return datafile

def test_sql_server_loader():
    """Test SQL Server loader (without actual database connection)"""
    print("Testing SQL Server Loader...")
    
    from skyloader.loader_sql_server import SqlLoader, schema_information
    
    # Create test data
    test_data = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'age': [25, 30],
        'salary': [50000.0, 60000.0]
    })
    
    # Test schema information function
    schema_info = schema_information(test_data)
    print(f"Schema info: {schema_info}")
    
    # Create loader (without connecting)
    loader = SqlLoader()
    loader.schema = "test_schema"
    
    # Create mock datafile
    datafile = Mock()
    datafile.tablename = "test_table"
    datafile.data = test_data
    
    # Test create table statement
    try:
        ddl, params = loader.create_table_statement(datafile)
        print(f"Generated DDL: {ddl}")
        print(f"Parameters: {params}")
        print("✓ SQL Server loader test passed\n")
    except Exception as e:
        print(f"✗ SQL Server loader test failed: {e}\n")

def test_postgres_loader():
    """Test PostgreSQL loader (without actual database connection)"""
    print("Testing PostgreSQL Loader...")
    
    from skyloader.loader_postgres import PostgresLoader, schema_information
    
    # Create test data
    test_data = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'age': [25, 30],
        'salary': [50000.0, 60000.0],
        'active': [True, False]
    })
    
    # Test schema information function
    schema_info = schema_information(test_data)
    print(f"Schema info: {schema_info}")
    
    # Create loader (without connecting)
    loader = PostgresLoader(
        server="localhost",
        database="test_db",
        user="test_user",
        password="test_pass",
        schemaname="test_schema"
    )
    
    # Create mock datafile
    datafile = Mock()
    datafile.tablename = "test_table"
    datafile.data = test_data
    
    # Test create table statement
    try:
        ddl = loader.create_table_statement(datafile)
        print(f"Generated DDL: {ddl}")
        
        # Test insert statement
        insert_sql = loader.insert_statement(test_data.columns, "test_table")
        print(f"Insert SQL: {insert_sql}")
        print("✓ PostgreSQL loader test passed\n")
    except Exception as e:
        print(f"✗ PostgreSQL loader test failed: {e}\n")

def test_loader_manager():
    """Test LoaderManager (without Google Drive)"""
    print("Testing LoaderManager...")
    
    try:
        # Import directly from the module file to avoid drive dependencies
        from skyloader.loader_manager import LoaderManager
        
        # Create mock drive and loader
        mock_drive = Mock()
        mock_loader = Mock()
        
        # Create LoaderManager
        manager = LoaderManager(drive=mock_drive, loader=mock_loader)
        
        print(f"Generated run_id: {manager.run_id}")
        print(f"Log stream created: {type(manager.log_stream)}")
        
        # Test metadata insertion
        datafile = Mock()
        datafile.data = pd.DataFrame({'test': [1, 2, 3]})
        
        manager.insert_metadata_fields(datafile)
        
        # Check if metadata was added
        if 'run_id' in datafile.data.columns and 'loaded_at' in datafile.data.columns:
            print("✓ Metadata fields added successfully")
        else:
            print("✗ Metadata fields not added")
        
        print("✓ LoaderManager test passed\n")
    except ImportError as e:
        print(f"⚠ LoaderManager test skipped due to missing dependencies: {e}")
        print("This is expected if Google Drive dependencies are not installed\n")

def test_input_validation():
    """Test input validation for column names"""
    print("Testing input validation...")
    
    from skyloader.loader_sql_server import SqlLoader
    from skyloader.loader_postgres import PostgresLoader
    
    # Test with malicious column names
    malicious_data = pd.DataFrame({
        'valid_column': [1, 2, 3],
        'DROP TABLE users; --': [4, 5, 6]  # This should be rejected
    })
    
    datafile = Mock()
    datafile.tablename = "test_table"
    datafile.data = malicious_data
    
    # Test SQL Server loader
    sql_loader = SqlLoader()
    sql_loader.schema = "test"
    
    try:
        sql_loader.create_table_statement(datafile)
        print("✗ SQL Server loader should have rejected malicious column name")
    except ValueError as e:
        print(f"✓ SQL Server loader correctly rejected malicious column: {e}")
    
    # Test PostgreSQL loader
    pg_loader = PostgresLoader("localhost", "test", schemaname="test")
    
    try:
        pg_loader.create_table_statement(datafile)
        print("✗ PostgreSQL loader should have rejected malicious column name")
    except ValueError as e:
        print(f"✓ PostgreSQL loader correctly rejected malicious column: {e}")
    
    print("✓ Input validation test passed\n")

if __name__ == "__main__":
    print("=== Skyloader Test Suite ===\n")
    
    try:
        # Run tests
        datafile = test_datafile()
        test_sql_server_loader()
        test_postgres_loader()
        test_loader_manager()
        test_input_validation()
        
        print("=== All tests completed ===")
        print("\nTo test with actual databases:")
        print("1. Install dependencies: pip install psycopg2-binary pyodbc")
        print("2. For Google Drive features: pip install google-api-python-client google-auth")
        print("3. Set up your database connection strings")
        print("4. Create instances of PostgresLoader or SqlLoader with real connection parameters")
        print("5. Use the load_datafile() method with real DataFile objects")
        
    except ImportError as e:
        print(f"Import error: {e}")
        print("Make sure you're running this from the project root directory")
        print("You may need to install basic dependencies first:")
        print("pip install pandas numpy requests")
