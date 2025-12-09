# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 15:44:37 2024

@author: jason.vogensen
"""

import os
import cx_Oracle
import configparser
import datetime

# Import project-specific components
from src.configuration_manager import ConfigurationManager
from src.configuration_manager.instance import get_config as get_config_manager
from base_data_project.log_config import get_logger

# Initialize logger with project name from config
logger = get_logger(get_config_manager().project_name)

def connect_to_oracle(acessos_path, log_file="log.txt"):
    """
    Establishes a connection to an Oracle database using credentials from an access file
    and sets the current schema.

    Parameters:
        acessos_path (str): Path to the 'acessos.txt' file with database credentials.
        log_file (str): Path to a log file where connection errors are recorded.

    Returns:
        connection (cx_Oracle.Connection): Oracle connection object if successful, None otherwise.
    """
    try:
        config = get_config_manager()
        
        # Check if database is enabled in configuration
        if not config.is_database_enabled:
            logger.warning("Database is not enabled in configuration")
            return None

        # Set up Oracle client path if required
        oracle_client_path = getattr(config.database, 'client_path', None)
        if oracle_client_path:
            os.environ["PATH"] = oracle_client_path + ";" + os.environ["PATH"]
        else:
            # Default Oracle client path
            os.environ["PATH"] = r"C:\oracle\instantclient_23_6;" + os.environ["PATH"]
        
        # Read configuration for database access
        config_parser = configparser.ConfigParser()
        config_file = os.path.join(acessos_path, "acessos.txt")
        
        if not os.path.isfile(config_file):
            raise FileNotFoundError(f"Configuration file not found at {config_file}")

        config_parser.read(config_file)
        
        # Extract database credentials and settings
        user = config_parser.get("Config", "user")
        passwd = config_parser.get("Config", "pwd")
        url = config_parser.get("Config", "url")
        port = config_parser.get("Config", "port")
        service_name = config_parser.get("Config", "service_name")
        
        # Create the DSN (Data Source Name) for Oracle
        dsn_tns = cx_Oracle.makedsn(url, port, service_name=service_name)
        
        # Establish connection
        connection = cx_Oracle.connect(user, passwd, dsn_tns, encoding="ISO-8859-1")
        logger.info("Connected to the Oracle database successfully")
        
        # Set the current schema
        schema = getattr(config.database, 'schema', 'WFM')
        with connection.cursor() as cursor:
            cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA={schema}")
            logger.info(f"Schema set to {schema}")
        
        return connection

    except Exception as e:
        # Log the error in a log file if connection fails
        error_message = f"{datetime.datetime.now().strftime('%d%m%Y %H:%M:%S')} ERROR CONNECTING TO DATABASE: {str(e)}"
        logger.error(error_message)
        
        try:
            with open(log_file, "a+") as myfile:
                myfile.write(error_message + "\n")
        except Exception as log_error:
            logger.error(f"Could not write to log file: {str(log_error)}")
        
        return None

def connect_to_oracle_with_config():
    """
    Connect to Oracle using configuration manager settings.
    
    Returns:
        connection (cx_Oracle.Connection): Oracle connection object if successful, None otherwise.
    """
    try:
        config = get_config_manager()
        
        # Check if database is enabled
        if not config.is_database_enabled:
            logger.warning("Database is not enabled in configuration")
            return None
        
        # Get database configuration
        if not hasattr(config, 'database'):
            logger.error("No database configuration available")
            return None
        
        # Extract connection parameters
        host = config.database.host
        port = getattr(config.database, 'port', 1521)
        service_name = getattr(config.database, 'service_name', 'XE')
        user = getattr(config.database, 'username', None)  # Note: attribute is 'username' not 'user'
        password = getattr(config.database, 'password', None)
        schema = getattr(config.database, 'schema', 'WFM')
        
        if not user or not password:
            logger.error("Database user and password must be configured")
            return None
        
        # Set up Oracle client path if configured
        oracle_client_path = getattr(config.database, 'client_path', None)
        if oracle_client_path:
            os.environ["PATH"] = oracle_client_path + ";" + os.environ["PATH"]
        
        # Create the DSN (Data Source Name) for Oracle
        dsn_tns = cx_Oracle.makedsn(host, port, service_name=service_name)
        
        # Establish connection
        connection = cx_Oracle.connect(user, password, dsn_tns, encoding="ISO-8859-1")
        logger.info(f"Connected to Oracle database at {host}:{port}/{service_name}")
        
        # Set the current schema
        with connection.cursor() as cursor:
            cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA={schema}")
            logger.info(f"Schema set to {schema}")
        
        return connection
        
    except Exception as e:
        logger.error(f"Error connecting to Oracle with config: {str(e)}")
        return None

def ensure_connection(connection, path):
    """
    Ensures that the database connection is active.
    
    Args:
        connection (cx_Oracle.Connection or None): Current database connection.
        path (str): Path for reconnecting if connection is lost.
    
    Returns:
        cx_Oracle.Connection: A valid database connection.
    """
    try:
        # Check if connection exists and is valid
        if connection is None:
            logger.info("No connection exists. Creating new connection...")
            connection = connect_to_oracle(path)
        else:
            # Check if the connection is valid and open
            connection.ping()  # Ping to test the connection
            logger.debug("Connection is active and healthy")
            
    except cx_Oracle.InterfaceError as e:
        logger.warning(f"Connection is closed or invalid: {e}. Reconnecting...")
        connection = connect_to_oracle(path)
    except cx_Oracle.DatabaseError as e:
        logger.warning(f"Database error: {e}. Reconnecting...")
        connection = connect_to_oracle(path)
    except AttributeError as e:
        logger.warning(f"Connection object is None: {e}. Creating new connection...")
        connection = connect_to_oracle(path)
    except Exception as e:
        logger.error(f"Unexpected error checking connection: {e}. Attempting reconnection...")
        connection = connect_to_oracle(path)
    
    return connection

def ensure_connection_with_config(connection):
    """
    Ensures that the database connection is active using configuration manager.
    
    Args:
        connection (cx_Oracle.Connection or None): Current database connection.
    
    Returns:
        cx_Oracle.Connection: A valid database connection.
    """
    try:
        # Check if connection exists and is valid
        if connection is None:
            logger.info("No connection exists. Creating new connection...")
            connection = connect_to_oracle_with_config()
        else:
            # Check if the connection is valid and open
            connection.ping()  # Ping to test the connection
            logger.debug("Connection is active and healthy")
            
    except cx_Oracle.InterfaceError as e:
        logger.warning(f"Connection is closed or invalid: {e}. Reconnecting...")
        connection = connect_to_oracle_with_config()
    except cx_Oracle.DatabaseError as e:
        logger.warning(f"Database error: {e}. Reconnecting...")
        connection = connect_to_oracle_with_config()
    except AttributeError as e:
        logger.warning(f"Connection object is None: {e}. Creating new connection...")
        connection = connect_to_oracle_with_config()
    except Exception as e:
        logger.error(f"Unexpected error checking connection: {e}. Attempting reconnection...")
        connection = connect_to_oracle_with_config()
    
    return connection

def test_connection(connection):
    """
    Test if a database connection is working properly.
    
    Args:
        connection (cx_Oracle.Connection): Database connection to test.
        
    Returns:
        bool: True if connection is working, False otherwise.
    """
    try:
        if connection is None:
            return False
        
        # Try to ping the connection
        connection.ping()
        
        # Try a simple query
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM DUAL")
            result = cursor.fetchone()
            
        logger.info("Database connection test successful")
        return True
        
    except Exception as e:
        logger.error(f"Database connection test failed: {str(e)}")
        return False

def disconnect_from_oracle(connection, log_file="log.txt"):
    """
    Closes the connection to the Oracle database.

    Parameters:
        connection (cx_Oracle.Connection): The database connection to be closed.
        log_file (str): Path to a log file where disconnection errors are recorded.
    
    Returns:
        bool: True if disconnected successfully, False otherwise.
    """
    try:
        if connection:
            connection.close()
            logger.info("Disconnected from the Oracle database successfully")
            return True
        else:
            logger.warning("No active connection to close")
            return False
            
    except Exception as e:
        error_message = f"{datetime.datetime.now().strftime('%d%m%Y %H:%M:%S')} ERROR DISCONNECTING FROM DATABASE: {str(e)}"
        logger.error(error_message)
        
        try:
            with open(log_file, "a+") as myfile:
                myfile.write(error_message + "\n")
        except Exception as log_error:
            logger.error(f"Could not write to log file: {str(log_error)}")
        
        return False

def get_connection_info(connection):
    """
    Get information about the current database connection.
    
    Args:
        connection (cx_Oracle.Connection): Database connection.
        
    Returns:
        dict: Connection information or None if connection is invalid.
    """
    try:
        if connection is None:
            return None
        
        connection_info = {
            'dsn': connection.dsn,
            'username': connection.username,
            'current_schema': None,
            'server_version': connection.version,
            'client_version': cx_Oracle.clientversion(),
            'is_healthy': False
        }
        
        # Test if connection is healthy
        try:
            connection.ping()
            connection_info['is_healthy'] = True
            
            # Get current schema
            with connection.cursor() as cursor:
                cursor.execute("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL")
                result = cursor.fetchone()
                if result:
                    connection_info['current_schema'] = result[0]
                    
        except:
            connection_info['is_healthy'] = False
        
        return connection_info
        
    except Exception as e:
        logger.error(f"Error getting connection info: {str(e)}")
        return None

# Context manager for database connections
class OracleConnectionManager:
    """Context manager for Oracle database connections."""
    
    def __init__(self, acessos_path: str = None, use_config: bool = True):
        """
        Initialize the connection manager.
        
        Args:
            acessos_path: Path to access configuration file
            use_config: Whether to use configuration manager for connection
        """
        self.acessos_path = acessos_path
        self.use_config = use_config
        self.connection = None
        
    def __enter__(self):
        """Enter the context manager and establish connection."""
        try:
            if self.use_config:
                self.connection = connect_to_oracle_with_config()
            else:
                if self.acessos_path is None:
                    raise ValueError("acessos_path is required when use_config=False")
                self.connection = connect_to_oracle(self.acessos_path)
            
            if self.connection is None:
                raise RuntimeError("Failed to establish database connection")
                
            return self.connection
            
        except Exception as e:
            logger.error(f"Error in connection manager enter: {str(e)}")
            raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager and close connection."""
        try:
            if self.connection:
                disconnect_from_oracle(self.connection)
                self.connection = None
        except Exception as e:
            logger.error(f"Error in connection manager exit: {str(e)}")

# Utility functions
def execute_query(connection, query: str, parameters: dict = None):
    """
    Execute a SQL query safely.
    
    Args:
        connection: Database connection
        query: SQL query to execute
        parameters: Query parameters (optional)
        
    Returns:
        Query results or None if error
    """
    try:
        with connection.cursor() as cursor:
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            
            # Fetch results for SELECT queries
            if query.strip().upper().startswith('SELECT'):
                return cursor.fetchall()
            else:
                connection.commit()
                return cursor.rowcount
                
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        connection.rollback()
        return None

def execute_query_to_dataframe(connection, query: str, parameters: dict = None):
    """
    Execute a SQL query and return results as pandas DataFrame.
    
    Args:
        connection: Database connection
        query: SQL query to execute
        parameters: Query parameters (optional)
        
    Returns:
        pandas.DataFrame with results or empty DataFrame if error
    """
    try:
        import pandas as pd
        
        with connection.cursor() as cursor:
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch data
            data = cursor.fetchall()
            
            # Create DataFrame
            df = pd.DataFrame(data, columns=columns)
            
            logger.info(f"Query executed successfully. Retrieved {len(df)} rows")
            return df
            
    except Exception as e:
        logger.error(f"Error executing query to DataFrame: {str(e)}")
        import pandas as pd
        return pd.DataFrame()