"""
System-level configuration management.

This module handles core system settings including:
- Project identification (name, environment)
- Database usage flags
- File system paths
- Algorithm configurations
- Logging settings

All system settings are loaded from src/settings/system_settings.py
"""

from typing import Dict, Any, List
from base_data_project.log_config import get_logger


class SystemConfig:
    """
    System-level configuration management.
    
    This class loads and validates core system settings that are required
    for all other configuration components to function properly.
    
    Required attributes (always present):
        project_name: str - Name of the project for logging and identification
        environment: str - Environment (dev, staging, prod, etc.)
        use_db: bool - Whether to use database or CSV files
        project_root_dir: str - Root directory path for the project
        
    Dynamic dictionaries (may be empty):
        storage_strategy: Dict[str, Any] - Storage configuration options
        available_algorithms: List[str] - List of available algorithm names
        logging_config: Dict[str, Any] - Logging configuration settings
        
    Additional settings:
        override_parameter_defaults: bool - Whether to override parameter defaults
    """
    
    def __init__(self):
        """
        Initialize system configuration.
        
        Note: This is the first config to load and establishes the project name
        that will be used for logging in all other configuration components.
        
        Raises:
            FileNotFoundError: If system_settings.py cannot be imported
            ValueError: If required fields are missing or invalid
        """
        # Load basic system config first (no logging yet)
        self._config_data = self._load_system_settings()
        
        # Extract required attributes
        self.project_name: str = self._config_data["project_name"]
        self.environment: str = self._config_data["environment"]
        self.use_db: bool = self._config_data["use_db"]
        self.project_root_dir: str = self._config_data["project_root_dir"]
        
        # Initialize logger with actual project name
        self.logger = get_logger(self.project_name)
        self.logger.info(f"System configuration loaded for project: {self.project_name}")
        
        # Extract dynamic dictionaries
        self.storage_strategy: Dict[str, Any] = self._config_data.get("storage_strategy", {})
        self.available_algorithms: List[str] = self._config_data.get("available_algorithms", [])
        self.logging_config: Dict[str, Any] = self._config_data.get("logging", {})
        
        # Additional system settings
        self.override_parameter_defaults: bool = self._config_data.get("override_parameter_defaults", False)
        
        # Validate all required fields
        self._validate_required_fields()
        
    def _load_system_settings(self) -> Dict[str, Any]:
        """
        Load system settings from the configuration file.
        
        Returns:
            Dict[str, Any]: Raw system configuration data
            
        Raises:
            FileNotFoundError: If system_settings.py cannot be imported
            ValueError: If the configuration data is invalid
        """
        try:
            from src.settings.system_settings import system_configs
            return system_configs
        except ImportError as e:
            raise FileNotFoundError(f"Could not import system_settings: {e}")
        except Exception as e:
            raise ValueError(f"Error loading system settings: {e}")
    
    def _validate_required_fields(self) -> None:
        """
        Validate that all required system fields are present and valid.
        
        Raises:
            ValueError: If any required field is missing or has invalid type
        """
        required_fields = [
            "project_name", "environment", "use_db", "project_root_dir",
            "storage_strategy", "available_algorithms", "logging"
        ]
        
        missing_fields = []
        for field in required_fields:
            if field not in self._config_data:
                missing_fields.append(field)
        
        if missing_fields:
            raise ValueError(f"System config missing required fields: {missing_fields}")
        
        # Validate specific field types
        if not isinstance(self.available_algorithms, list):
            raise ValueError("available_algorithms must be a list")
        
        if not isinstance(self.storage_strategy, dict):
            raise ValueError("storage_strategy must be a dictionary")
            
        if not isinstance(self.logging_config, dict):
            raise ValueError("logging configuration must be a dictionary")
        
        # Validate required string fields are not empty
        string_fields = ["project_name", "environment", "project_root_dir"]
        for field in string_fields:
            if not self._config_data[field] or not isinstance(self._config_data[field], str):
                raise ValueError(f"{field} must be a non-empty string")
        
        self.logger.info("System configuration validation passed")
    
    def get_log_level(self) -> str:
        """
        Get the configured log level.
        
        Returns:
            str: Log level (INFO, DEBUG, WARNING, etc.)
        """
        return self.logging_config.get("log_level", "INFO")
    
    def is_development_environment(self) -> bool:
        """
        Check if running in development environment.
        
        Returns:
            bool: True if environment is 'dev' or 'development'
        """
        return self.environment.lower() in ["dev", "development"]
    
    def is_production_environment(self) -> bool:
        """
        Check if running in production environment.
        
        Returns:
            bool: True if environment is 'prod' or 'production'
        """
        return self.environment.lower() in ["prod", "production"]
    
    def get_algorithm_list(self) -> List[str]:
        """
        Get the list of available algorithms.
        
        Returns:
            List[str]: List of algorithm names
        """
        return self.available_algorithms.copy()
    
    def has_algorithm(self, algorithm_name: str) -> bool:
        """
        Check if a specific algorithm is available.
        
        Args:
            algorithm_name: Name of the algorithm to check
            
        Returns:
            bool: True if algorithm is in the available list
        """
        return algorithm_name in self.available_algorithms
    
    def __repr__(self) -> str:
        """String representation of SystemConfig."""
        return (f"SystemConfig(project='{self.project_name}', "
                f"environment='{self.environment}', "
                f"use_db={self.use_db})")