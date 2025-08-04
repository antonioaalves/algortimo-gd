"""File containing the system settings"""

# Dependencies
import os

# Local stuff
from src.settings.log_parameters import log_parameters

system_configs = {
    "environment": "alcampo_tst", # Options: development, production
    "use_db": True, # Options: True, False
    "override_parameter_defaults": False, # Options: True, False
    
    "project_name": log_parameters.get("project_name", 'algoritmo_GD'), # Important for environment management
    "project_version": "1.1-dev", # Important for environment management
    "project_author": "Tlantic SI - Strategic Solutions Team", # Important for environment management
    "project_author_url": "https://github.com/antonioaalves/algortimo-gd", # Important for environment management
    "project_license": "MIT",
    "project_root_dir": os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),

    "storage_strategy": {
        "mode": "memory", # Options: memory, persist, hybrid
        "persist_intermediate_results": False, # Options: True, False
        "stages_to_persist": [], # Empty list means all stages
        "cleanup_policy": "keep_latest", # Options: keep_all, keep_latest, keep_none
        "persist_format": "", # Options: csv, db
        "storage_dir": "data/intermediate" # For CSV storage 
    },

    "logging": {
        'environment': 'server',  # or 'local' for development
        'db_logging_enabled': True,
        'df_messages_path': 'data/csvs/messages.csv',
        'log_errors_db': True,  # Enable/disable database error logging with set_process_errors
        'log_level': 'INFO',
        'log_dir': 'logs',
    },

    "available_algorithms": [
        "alcampo_algorithm",
        "salsa_algorithm",
    ],

}