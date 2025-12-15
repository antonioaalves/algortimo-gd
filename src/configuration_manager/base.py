"""File containing the base configuration manager class"""

# Dependencies
from abc import abstractmethod

# Local stuff
from base_data_project.log_config import get_logger

class BaseConfig:
    """Class used to import the base configuration"""

    def __init__(self):
        """Initialize the base configuration"""
        self.logger = get_logger(project_name="algoritmo_GD")

        self.base_config = {}
        self.path_config = {}
        self.parameter_config = {}
        self.stages_config = {}

    @abstractmethod
    def load_base_config(self):
        """Load the base configuration"""
        pass

    @abstractmethod
    def load_path_config(self):
        """Load the path configuration"""
        pass

    @abstractmethod
    def load_parameter_config(self):
        """Load the parameter configuration"""
        pass

    @abstractmethod
    def load_stages_config(self):
        """Load the stages configuration"""
        pass

    @abstractmethod
    def load_oracle_config(self):
        """Load the oracle configuration""" 
        pass

    