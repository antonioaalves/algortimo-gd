"""File containing the base configuration import class"""

# Local stuff
from base_data_project.log_config import get_logger

class SystemConfig:
    """Class used to import the base configuration"""

    def __init__(self):
        """Initialize the base configuration"""
        self.logger = get_logger(project_name="algoritmo_GD")

        self.base_config = self.load_base_config()

        # Validate the base configuration
        if not self.validate_base_config():
            raise ValueError("Base configuration validation failed")

    def __getitem__(self, key):
        """Allow dictionary-style access to config values"""
        return self.base_config[key]
    
    def __contains__(self, key):
        """Allow 'in' operator to check if key exists"""
        return key in self.base_config
    
    def get(self, key, default=None):
        """Safe dictionary-style access with default value"""
        return self.base_config.get(key, default)
    
    def keys(self):
        """Return config keys"""
        return self.base_config.keys()
    
    def items(self):
        """Return config items"""
        return self.base_config.items()

    def load_base_config(self):
        """Load the configuration file"""
        # TODO: Implement the loading function for python config files
        try:
            from src.settings.system_settings import system_configs
            self.logger.info(f"Base config loaded successfully: {system_configs}")
            return system_configs
        except Exception as e:
            self.logger.error(f"Error loading base config: {e}")
            raise e

    def validate_base_config(self):
        """Validate the base configuration"""
        # Validate the base configuration
        needed_keys = [
            "environment",
            "use_db",
            "override_parameter_defaults",
            "project_name",
            "project_root_dir",
            "storage_strategy",
            "available_algorithms",
            "logging",
        ]

        # Validate the base configuration
        for key in needed_keys:
            if key not in self.base_config.keys():
                self.logger.error(f"Base config is missing the key: {key}")
                raise ValueError(f"Base config is missing the key: {key}")

        return True