"""File containing the system settings"""

# Dependencies
import os

# Local stuff
from src.settings.log_parameters import log_parameters


def _read_version() -> str:
    """Read the project version from the VERSION file at the repo root."""
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    version_file = os.path.join(root, "VERSION")
    try:
        with open(version_file, "r", encoding="utf-8") as fh:
            return fh.read().strip() or "0.0.0"
    except FileNotFoundError:
        return "0.0.0"

system_configs = {
    "environment": "salsa_tst", # Options: development, production
    "use_db": True, # Options: True, False
    "override_parameter_defaults": False, # Options: True, False
    "granularity": 15,
    
    "project_name": log_parameters.get("project_name", 'algoritmo_GD'), # Important for environment management
    "project_version": _read_version(), # Read from the VERSION file at repo root
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