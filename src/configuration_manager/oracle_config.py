"""File containing the oracle credentials configuration treatment"""

# Dependencies
import json
import os

# Local stuff
from base_data_project.log_config import get_logger

class OracleConfig:
    """Class used to import the oracle credentials configuration"""

    def __init__(self, environment: str):
        """Initialize the oracle credentials configuration"""
        self.logger = get_logger(project_name="algoritmo_GD")
        self.environment = environment

        self.oracle_config_dict = self.load_oracle_config(environment=environment)

        if not self.validate_oracle_config():
            raise ValueError("Oracle credentials configuration validation failed")

    def __getitem__(self, key):
        """Allow dictionary-style access to config values"""
        return self.oracle_config_dict[key]
    
    def __contains__(self, key):
        """Allow 'in' operator to check if key exists"""
        return key in self.oracle_config_dict
    
    def get(self, key, default=None):
        """Safe dictionary-style access with default value"""
        return self.oracle_config_dict.get(key, default)
    
    def keys(self):
        """Return config keys"""
        return self.oracle_config_dict.keys()
    
    def items(self):
        """Return config items"""
        return self.oracle_config_dict.items()

    def load_oracle_config(self, environment: str):
        """Load the oracle credentials configuration"""
        try:
            with open("src/settings/oracle_connection_parameters.json", "r") as file:
                oracle_config_dict = json.load(file)
                self.logger.info(f"Oracle credentials configuration loaded successfully for environment: {environment}")
        except FileNotFoundError:
            self.logger.error(f"Oracle connection parameters file not found")
            raise FileNotFoundError(f"Oracle connection parameters file not found")
        except Exception as e:
            self.logger.error(f"Error loading oracle credentials configuration: {e}")
            raise e

        if environment not in oracle_config_dict:
            self.logger.error(f"Environment '{environment}' not found in oracle configuration")
            raise ValueError(f"Environment '{environment}' not found in oracle configuration")

        oracle_config = oracle_config_dict[environment]
        return oracle_config

    def validate_oracle_config(self):
        """Validate the oracle credentials configuration"""
        try:
            required_keys = ["host", "port", "service_name", "username", "password", "schema"]
            
            for key in required_keys:
                if key not in self.oracle_config_dict:
                    self.logger.error(f"Oracle config is missing required key: {key}")
                    return False
                
                if not self.oracle_config_dict[key]:
                    self.logger.error(f"Oracle config has empty value for key: {key}")
                    return False

            self.logger.info("Oracle credentials configuration validation successful")
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating oracle credentials configuration: {e}")
            return False

    def get_connection_url(self) -> str:
        """Generate SQLAlchemy connection URL for base-data-project"""
        try:
            config = self.oracle_config_dict
            connection_url = (f"oracle+cx_oracle://{config['username']}:"
                            f"{config['password']}@"
                            f"{config['host']}:{config['port']}/"
                            f"?service_name={config['service_name']}")
            
            self.logger.info(f"Generated Oracle connection URL for environment: {self.environment}")
            return connection_url
            
        except Exception as e:
            self.logger.error(f"Error generating Oracle connection URL: {e}")
            raise e

    def get_legacy_config(self) -> dict:
        """Get config in legacy format for compatibility with existing connect.py"""
        try:
            config = self.oracle_config_dict
            legacy_config = {
                "user": config["username"],
                "pwd": config["password"], 
                "url": config["host"],
                "port": config["port"],
                "service_name": config["service_name"]
            }
            
            self.logger.info(f"Generated legacy Oracle config for environment: {self.environment}")
            return legacy_config
            
        except Exception as e:
            self.logger.error(f"Error generating legacy Oracle config: {e}")
            raise e

    @property
    def db_url(self) -> str:
        """Get database URL for base-data-project integration"""
        try:
            return self.get_connection_url()
        except Exception as e:
            self.logger.error(f"Error getting database URL: {e}")
            return ""