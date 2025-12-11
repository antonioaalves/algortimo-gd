"""
Database connection configuration management.

This module handles Oracle database connection settings including:
- Connection parameters (host, port, service name)
- Authentication credentials (username, password)
- Schema information
- Connection URL generation

Configuration is loaded from src/settings/oracle_connection_parameters.json
based on the specified environment (dev, staging, prod, etc.).
"""

import json
from typing import Dict, Any
from base_data_project.log_config import get_logger


class DatabaseConfig:
    """
    Database connection configuration management.
    
    This class loads environment-specific Oracle database connection settings
    and provides methods for generating connection URLs and validating credentials.
    
    Required attributes (always present):
        host: str - Database server hostname or IP
        port: int - Database server port number
        service_name: str - Oracle service name
        username: str - Database username
        password: str - Database password (stored securely)
        schema: str - Default schema name
        
    Additional attributes:
        environment: str - Environment this config is for
        logger: Logger instance for this component
    """
    
    def __init__(self, environment: str, project_name: str):
        """
        Initialize database configuration for the specified environment.
        
        Args:
            environment: Environment name (dev, staging, prod, etc.)
            project_name: Project name for logging purposes
            
        Raises:
            FileNotFoundError: If oracle_connection_parameters.json is missing
            ValueError: If environment not found or required fields missing
            json.JSONDecodeError: If JSON file is malformed
        """
        self.logger = get_logger(project_name)
        self.environment = environment
        
        # Load and validate database config
        self._config_data = self._load_database_config()
        
        # Extract required attributes
        self.host: str = self._config_data["host"]
        self.port: int = self._config_data["port"]
        self.service_name: str = self._config_data["service_name"]
        self.username: str = self._config_data["username"]
        self.password: str = self._config_data["password"]
        self.schema: str = self._config_data["schema"]
        
        self._validate_required_fields()
        self.logger.info(f"Database configuration loaded for environment: {environment}")
    
    def _load_database_config(self) -> Dict[str, Any]:
        """
        Load database configuration for the specified environment.
        
        Returns:
            Dict[str, Any]: Database configuration for the environment
            
        Raises:
            FileNotFoundError: If configuration file doesn't exist
            ValueError: If environment not found in configuration
            json.JSONDecodeError: If JSON is malformed
        """
        try:
            with open("src/settings/oracle_connection_parameters.json", "r") as file:
                all_configs = json.load(file)
            
            if self.environment not in all_configs:
                available_envs = list(all_configs.keys())
                raise ValueError(
                    f"Environment '{self.environment}' not found in database config. "
                    f"Available environments: {available_envs}"
                )
            
            return all_configs[self.environment]
            
        except FileNotFoundError:
            raise FileNotFoundError(
                "Database configuration file 'oracle_connection_parameters.json' not found"
            )
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in database configuration: {e}")
    
    def _validate_required_fields(self) -> None:
        """
        Validate that all required database fields are present and valid.
        
        Raises:
            ValueError: If any required field is missing or invalid
        """
        required_fields = ["host", "port", "service_name", "username", "password", "schema"]
        
        missing_fields = []
        for field in required_fields:
            if field not in self._config_data:
                missing_fields.append(field)
        
        if missing_fields:
            raise ValueError(f"Database config missing required fields: {missing_fields}")
        
        # Validate field types and values
        if not isinstance(self.port, int) or self.port <= 0:
            raise ValueError("Database port must be a positive integer")
        
        # Validate required string fields are not empty
        string_fields = ["host", "service_name", "username", "password", "schema"]
        for field in string_fields:
            value = getattr(self, field)
            if not value or not isinstance(value, str):
                raise ValueError(f"Database {field} must be a non-empty string")
        
        self.logger.info("Database configuration validation passed")
    
    def get_connection_url(self) -> str:
        """
        Generate Oracle connection URL for SQLAlchemy/cx_Oracle.
        
        Returns:
            str: Complete Oracle connection URL
            
        Raises:
            Exception: If URL generation fails
        """
        try:
            connection_url = (f"oracle+cx_oracle://{self.username}:"
                            f"{self.password}@"
                            f"{self.host}:{self.port}/"
                            f"?service_name={self.service_name}")
            
            self.logger.info("Database connection URL generated successfully")
            return connection_url
            
        except Exception as e:
            self.logger.error(f"Error generating Oracle connection URL: {e}")
            raise e
    
    def get_connection_params(self) -> Dict[str, Any]:
        """
        Get connection parameters as a dictionary.
        
        Returns:
            Dict[str, Any]: Connection parameters (password masked in logs)
        """
        params = {
            "host": self.host,
            "port": self.port,
            "service_name": self.service_name,
            "username": self.username,
            "schema": self.schema,
            "environment": self.environment
        }
        
        self.logger.info(f"Retrieved connection parameters for {self.environment}")
        return params
    
    def get_legacy_config(self) -> Dict[str, Any]:
        """
        Get configuration in legacy format for backward compatibility.
        
        This method provides the config in the format expected by older
        connection utilities or external libraries.
        
        Returns:
            Dict[str, Any]: Legacy-format configuration
        """
        try:
            legacy_config = {
                "user": self.username,
                "pwd": self.password, 
                "url": self.host,
                "port": self.port,
                "service_name": self.service_name
            }
            
            self.logger.info(f"Generated legacy Oracle config for environment: {self.environment}")
            return legacy_config
            
        except Exception as e:
            self.logger.error(f"Error generating legacy Oracle config: {e}")
            raise e
    
    def test_connection_params(self) -> bool:
        """
        Validate that connection parameters are properly formatted.
        
        Note: This only validates format, not actual connectivity.
        Use this for configuration validation, not connection testing.
        
        Returns:
            bool: True if parameters are properly formatted
        """
        try:
            # Test URL generation
            url = self.get_connection_url()
            
            # Basic format validation
            if not url.startswith("oracle+cx_oracle://"):
                return False
            
            if "@" not in url or ":" not in url:
                return False
            
            self.logger.info("Connection parameters format validation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Connection parameters validation failed: {e}")
            return False
    
    def mask_sensitive_info(self) -> Dict[str, Any]:
        """
        Get configuration with sensitive information masked for logging.
        
        Returns:
            Dict[str, Any]: Configuration with password masked
        """
        return {
            "host": self.host,
            "port": self.port,
            "service_name": self.service_name,
            "username": self.username,
            "password": "***MASKED***",
            "schema": self.schema,
            "environment": self.environment
        }
    
    def __repr__(self) -> str:
        """String representation of DatabaseConfig with masked password."""
        return (f"DatabaseConfig(host='{self.host}', "
                f"port={self.port}, "
                f"service='{self.service_name}', "
                f"user='{self.username}', "
                f"environment='{self.environment}')")