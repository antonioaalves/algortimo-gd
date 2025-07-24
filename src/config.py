"""Configuration compatibility layer for the algoritmo_GD project."""

# Import the new configuration manager
from src.configuration_manager.manager import ConfigurationManager

# Create a global instance of the configuration manager
_config_manager = None

def get_config_manager():
    """Get or create the global configuration manager instance."""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigurationManager()
    return _config_manager

# Project name - used for logging and process tracking
PROJECT_NAME = "algoritmo_GD"

# Get application root directory
import os
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parents[1]

# Create a CONFIG dictionary that provides backward compatibility
def get_config():
    """Get the configuration dictionary with backward compatibility."""
    config_manager = get_config_manager()
    
    # Build the CONFIG dictionary from the new configuration manager
    config = {
        # Database configuration
        'use_db': config_manager.system_config.get('use_db', True),
        'db_url': config_manager.oracle_config.get_connection_url() if config_manager.oracle_config else None,
        
        # Base directories
        'data_dir': os.path.join(config_manager.system_config.get('project_root_dir'), 'data'),
        'output_dir': os.path.join(config_manager.system_config.get('project_root_dir'), 'data', 'output'),
        'log_dir': os.path.join(config_manager.system_config.get('project_root_dir'), 'logs'),

        # Storage strategy
        'storage_strategy': config_manager.system_config.get('storage_strategy', {}),

        # Logging configuration
        'logging': config_manager.system_config.get('logging', {}),
        
        # External call data
        'external_call_data': config_manager.parameter_config.get('external_call_data', {}),

        # Parameters names and defaults
        'parameters_names': config_manager.parameter_config.process_parameters.get('parameters_names', []),
        'parameters_defaults': config_manager.parameter_config.process_parameters.get('parameters_defaults', {}),

        # File paths for CSV data sources (dummy data)
        'dummy_data_filepaths': config_manager.path_config.dummy_data_filepaths,
        
        # Available entities for processing
        'available_entities_processing': config_manager.path_config.available_entities_processing,
        'available_entities_aux': config_manager.path_config.available_entities_aux,
        'available_entities_raw': config_manager.path_config.available_entities_raw,

        # Available algorithms
        'available_algorithms': config_manager.system_config.get('available_algorithms', []),
        
        # Process configuration - stages and decision points
        'stages': config_manager.stages_config.stages,
        
        # Algorithm parameters (defaults for each algorithm)
        'algorithm_defaults': config_manager.parameter_config.get('algorithm_defaults', {}),
        
        # Output configuration
        'output': {
            'base_dir': 'data/output',
            'visualizations_dir': 'data/output/visualizations',
            'diagnostics_dir': 'data/diagnostics'
        },
        
        # Logging configuration
        'log_level': config_manager.system_config.get('logging', {}).get('log_level', 'INFO'),
        'log_format': '%(asctime)s | %(levelname)8s | %(filename)s:%(lineno)d | %(message)s',
        'log_dir': 'logs',
        'console_output': True
    }
    
    return config

# Create the CONFIG object
CONFIG = get_config()

# Export the configuration manager for direct access
def get_configuration_manager():
    """Get the configuration manager instance for advanced usage."""
    return get_config_manager() 