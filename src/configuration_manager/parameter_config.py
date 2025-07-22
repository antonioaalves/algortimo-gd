"""File containing the parameter configuration class"""

# Dependencies
import json
from typing import Dict, Any

# Local stuff
from base_data_project.log_config import get_logger

class ParameterConfig:
    """Class used to manage the parameters needed for the project"""

    def __init__(self):
        """Initialize the parameter configuration"""
        self.logger = get_logger(project_name="algoritmo_GD")

        # Load the parameter configuration
        self.parameter_config_dict = self.load_parameter_config()

        # Validate the parameter configuration
        if not self.validate_parameter_config():
            raise ValueError("Parameter configuration validation failed")

    def __getitem__(self, key):
        """Allow dictionary-style access to config values"""
        return self.parameter_config_dict[key]
    
    def __contains__(self, key):
        """Allow 'in' operator to check if key exists"""
        return key in self.parameter_config_dict
    
    def get(self, key, default=None):
        """Safe dictionary-style access with default value"""
        return self.parameter_config_dict.get(key, default)
    
    def keys(self):
        """Return config keys"""
        return self.parameter_config_dict.keys()
    
    def items(self):
        """Return config items"""
        return self.parameter_config_dict.items()

    def load_parameter_config(self) -> Dict[str, Any]:
        """Load the parameter configuration from JSON file"""
        try:
            with open("src/settings/process_parameters.json", "r") as file:
                parameter_config_dict = json.load(file)
                self.logger.info(f"Process parameters configuration loaded successfully")
                return parameter_config_dict
        except FileNotFoundError:
            self.logger.error(f"Process parameters file not found")
            raise FileNotFoundError(f"Process parameters file not found")
        except Exception as e:
            self.logger.error(f"Error loading process parameters configuration: {e}")
            raise e

    def validate_parameter_config(self) -> bool:
        """Validate the parameter configuration"""
        try:
            required_keys = ['process_parameters', 'external_call_data']
            
            for key in required_keys:
                if key not in self.parameter_config_dict:
                    self.logger.error(f"Parameter config is missing required key: {key}")
                    return False

            # Validate process_parameters structure
            process_params = self.parameter_config_dict['process_parameters']
            if 'parameters_names' not in process_params or 'parameters_defaults' not in process_params:
                self.logger.error("process_parameters must contain parameters_names and parameters_defaults")
                return False

            self.logger.info("Process parameters configuration validation successful")
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating process parameters configuration: {e}")
            return False

    @property
    def process_parameters(self) -> Dict[str, Any]:
        """Access process parameters with same interface as CONFIG"""
        return self.parameter_config_dict.get('process_parameters', {})

    @property
    def external_call_data(self) -> Dict[str, Any]:
        """Access external call data"""
        return self.parameter_config_dict.get('external_call_data', {})