# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 14:47:33 2025

@author: jason.vogensen
"""
import os
from src.orquestrador_functions.Classes.Connection.connect import connect_to_oracle, ensure_connection, disconnect_from_oracle
 
class ConnectionHandler:
    def __init__(self, path):
        self.path = path
        self.connection = None
        
    def connect_to_database(self):
         """Establish connection to the database."""
         connection_path = os.path.join(self.path, "src", "orquestrador_functions", "Classes", "Connection")
         self.connection = connect_to_oracle(connection_path)
         self.connection = ensure_connection(self.connection, connection_path)

    def disconnect_database(self):
         """Close the database connection."""
         self.connection = disconnect_from_oracle(self.connection)

    # Expose connection 
    def get_connection(self):
         """Return the database connection."""
         return self.connection
     
    def ensure_connection(self):
        connection_path = os.path.join(self.path, "src", "orquestrador_functions", "Classes", "Connection")
        self.connection = ensure_connection(self.connection, connection_path)
        return(self.connection)