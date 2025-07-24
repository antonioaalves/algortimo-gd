"""
Configuration Manager Package

Provides clean, industry-standard configuration management for the algoritmo_GD project.

This package implements a hierarchical configuration system with:
- Strict validation and fail-fast behavior
- Clean typed access patterns (config.system.project_name)
- Proper logging lifecycle management
- Separation between required attributes and dynamic dictionaries

Usage Examples:
    # Basic usage - most common
    from src.configuration_manager import ConfigurationManager
    
    config = ConfigurationManager()
    
    # System configuration
    project_name = config.system.project_name
    use_database = config.system.use_db
    algorithms = config.system.available_algorithms
    
    # Database configuration (only if enabled)
    if config.is_database_enabled:
        db_url = config.database.get_connection_url()
        host = config.database.host
        port = config.database.port
    
    # Paths configuration (different based on use_db)
    if config.system.use_db:
        processing_paths = config.paths.sql_processing_paths
        aux_paths = config.paths.sql_auxiliary_paths
    else:
        csv_paths = config.paths.csv_filepaths
    
    # Parameters configuration
    process_params = config.parameters.process_parameters
    external_data = config.parameters.external_call_data
    
    # Stages configuration
    workflow_stages = config.stages.stages

Advanced Usage:
    # Import individual config classes for specialized use cases
    from src.configuration_manager import SystemConfig, DatabaseConfig
    
    # Or import specific configs only
    from src.configuration_manager.system_config import SystemConfig
    from src.configuration_manager.database_config import DatabaseConfig
    
    # Use individual components
    system = SystemConfig()
    if system.use_db:
        database = DatabaseConfig(system.environment, system.project_name)

Architecture:
    - ConfigurationManager: Main orchestrator class
    - SystemConfig: Core system settings (project_name, environment, etc.)
    - DatabaseConfig: Oracle database connection settings (conditional)
    - PathsConfig: File paths for CSV/SQL data sources
    - ParametersConfig: Process parameters and external call data
    - StagesConfig: Workflow stages and process structure
"""

from .manager import ConfigurationManager
from .system_config import SystemConfig
from .database_config import DatabaseConfig
from .paths_config import PathsConfig
from .parameters_config import ParametersConfig
from .stages_config import StagesConfig

# Version info
__version__ = "1.0.0"
__author__ = "Strategic Solutions Team - António Alves"

# Public API - what gets imported with "from src.configuration_manager import *"
__all__ = [
    # Main class - primary entry point
    'ConfigurationManager',
    
    # Individual config classes - for advanced usage
    'SystemConfig',
    'DatabaseConfig', 
    'PathsConfig',
    'ParametersConfig',
    'StagesConfig',
]

# Package-level convenience functions
def create_config_manager() -> ConfigurationManager:
    """
    Factory function to create a ConfigurationManager instance.
    
    This is equivalent to ConfigurationManager() but provides a more explicit API.
    Useful for dependency injection or when you want to make config creation explicit.
    
    Returns:
        ConfigurationManager: Fully initialized configuration manager
        
    Raises:
        FileNotFoundError: If configuration files are missing
        ValueError: If configuration validation fails
    """
    return ConfigurationManager()


def get_project_name() -> str:
    """
    Quick utility to get just the project name without loading full config.
    
    Returns:
        str: The project name from system settings
        
    Raises:
        FileNotFoundError: If system_settings.py is missing
        ValueError: If project_name is not defined
    """
    try:
        from src.settings.system_settings import system_configs
        return system_configs["project_name"]
    except (ImportError, KeyError) as e:
        raise ValueError(f"Could not load project name: {e}")


def validate_config_files() -> bool:
    """
    Utility function to validate that all required configuration files exist.
    
    This can be used for health checks or startup validation without fully
    initializing the configuration manager.
    
    Returns:
        bool: True if all required files exist and are valid
        
    Raises:
        FileNotFoundError: If any required configuration file is missing
        ValueError: If any configuration file has invalid content
    """
    try:
        # Try to create a config manager - this will validate everything
        config = ConfigurationManager()
        return config.validate_all_configs()
    except Exception:
        # Re-raise the specific exception for better error handling
        raise


# Package metadata for introspection
_PACKAGE_INFO = {
    "name": "configuration_manager",
    "version": __version__,
    "description": "Industry-standard configuration management for algoritmo_GD",
    "config_files": [
        "src/settings/system_settings.py",
        "src/settings/oracle_connection_parameters.json", 
        "src/settings/folder_hierarchy.json",
        "src/settings/sql_filepaths.json",
        "src/settings/process_parameters.json",
        "src/settings/project_structure.py"
    ],
    "main_classes": __all__,
}


def get_package_info() -> dict:
    """
    Get package metadata and information.
    
    Returns:
        dict: Package information including version, config files, etc.
    """
    return _PACKAGE_INFO.copy()