# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 14:47:33 2025

@author: jason.vogensen

Updated: Dec 2025 - Migrated to use config_manager instead of acessos.txt file
"""
from src.orquestrador_functions.Classes.Connection.connect import (
    connect_to_oracle_with_config, 
    ensure_connection_with_config, 
    disconnect_from_oracle
)
 
class ConnectionHandler:
    """
    Database connection handler that uses config_manager for credentials.
    
    This replaces the old approach of reading from acessos.txt file.
    Database credentials are now managed through oracle_connection_parameters.json
    via the centralized config_manager.
    """
    
    def __init__(self):
        """Initialize the connection handler (no path required - uses config_manager)."""
        self.connection = None
        
    def connect_to_database(self):
        """Establish connection to the database using config_manager credentials."""
        self.connection = connect_to_oracle_with_config()
        self.connection = ensure_connection_with_config(self.connection)

    def disconnect_database(self):
        """Close the database connection."""
        self.connection = disconnect_from_oracle(self.connection)

    def get_connection(self):
        """Return the database connection."""
        return self.connection
     
    def ensure_connection(self):
        """Ensure the connection is active, reconnecting if necessary."""
        self.connection = ensure_connection_with_config(self.connection)
        return self.connection