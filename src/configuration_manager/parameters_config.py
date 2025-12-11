"""
Process parameters and external call data configuration management.

This module handles dynamic parameter configurations including:
- Process parameters (algorithm parameters, defaults, names)
- External call data (API endpoints, external service configurations)

Configuration is loaded from src/settings/process_parameters.json
"""

import json
from typing import Dict, Any, List, Optional
from base_data_project.log_config import get_logger


class ParametersConfig:
    """
    Process parameters and external call data configuration management.
    
    This class manages dynamic parameter configurations that can vary
    based on algorithms, external services, and runtime requirements.
    
    Dynamic dictionaries (structure depends on configuration):
        process_parameters: Dict[str, Any] - Core process parameters including:
            - parameters_names: List[str] - List of parameter names
            - parameters_defaults: Dict[str, Any] - Default parameter values
            - algorithm_defaults: Dict[str, Any] - Algorithm-specific defaults (optional)
        external_call_data: Dict[str, Any] - External service configurations:
            - API endpoints, authentication, timeout settings, etc.
    """
    
    def __init__(self, project_name: str):
        """
        Initialize parameters configuration.
        
        Args:
            project_name: Project name for logging purposes
            
        Raises:
            FileNotFoundError: If process_parameters.json is missing
            ValueError: If configuration validation fails
            json.JSONDecodeError: If JSON file is malformed
        """
        self.logger = get_logger(project_name)
        
        # Load parameter configuration
        self._config_data = self._load_parameters_config()
        
        # Extract dynamic dictionaries (these have variable structure)
        self.process_parameters: Dict[str, Any] = self._config_data.get("process_parameters", {})
        self.external_call_data: Dict[str, Any] = self._config_data.get("external_call_data", {})
        
        self._validate_required_fields()
        self.logger.info("Parameters configuration loaded successfully")
    
    def _load_parameters_config(self) -> Dict[str, Any]:
        """
        Load parameters configuration from JSON file.
        
        Returns:
            Dict[str, Any]: Complete parameters configuration
            
        Raises:
            FileNotFoundError: If configuration file doesn't exist
            json.JSONDecodeError: If JSON is malformed
        """
        try:
            with open("src/settings/process_parameters.json", "r") as file:
                config = json.load(file)
            self.logger.info("Parameters configuration file loaded successfully")
            return config
        except FileNotFoundError:
            raise FileNotFoundError("Parameters file 'process_parameters.json' not found")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in parameters configuration: {e}")
    
    def _validate_required_fields(self) -> None:
        """
        Validate that required parameter sections exist and have correct structure.
        
        Raises:
            ValueError: If required sections are missing or invalid
        """
        required_sections = ["process_parameters", "external_call_data"]
        
        missing_sections = []
        for section in required_sections:
            if section not in self._config_data:
                missing_sections.append(section)
        
        if missing_sections:
            raise ValueError(f"Parameters config missing required sections: {missing_sections}")
        
        # Validate process_parameters structure
        if not isinstance(self.process_parameters, dict):
            raise ValueError("process_parameters must be a dictionary")
        
        if "parameters_names" not in self.process_parameters:
            raise ValueError("process_parameters must contain 'parameters_names'")
        if "parameters_defaults" not in self.process_parameters:
            raise ValueError("process_parameters must contain 'parameters_defaults'")
        
        # Validate parameters_names is a list
        if not isinstance(self.process_parameters["parameters_names"], list):
            raise ValueError("parameters_names must be a list")
        
        # Validate parameters_defaults is a dictionary
        if not isinstance(self.process_parameters["parameters_defaults"], dict):
            raise ValueError("parameters_defaults must be a dictionary")
        
        # Validate external_call_data structure
        if not isinstance(self.external_call_data, dict):
            raise ValueError("external_call_data must be a dictionary")
        
        self.logger.info("Parameters configuration validation passed")
    
    def get_parameter_names(self) -> List[str]:
        """
        Get the list of available parameter names.
        
        Returns:
            List[str]: List of parameter names
        """
        return self.process_parameters.get("parameters_names", []).copy()
    
    def get_parameter_defaults(self) -> Dict[str, Any]:
        """
        Get the default values for parameters.
        
        Returns:
            Dict[str, Any]: Parameter name to default value mapping
        """
        return self.process_parameters.get("parameters_defaults", {}).copy()
    
    def get_algorithm_defaults(self) -> Dict[str, Any]:
        """
        Get algorithm-specific default parameters.
        
        Returns:
            Dict[str, Any]: Algorithm-specific defaults (empty if not configured)
        """
        return self.process_parameters.get("algorithm_defaults", {}).copy()
    
    def get_parameter_default(self, parameter_name: str) -> Any:
        """
        Get the default value for a specific parameter.
        
        Args:
            parameter_name: Name of the parameter
            
        Returns:
            Any: Default value for the parameter
            
        Raises:
            KeyError: If parameter name is not found
        """
        defaults = self.get_parameter_defaults()
        if parameter_name not in defaults:
            raise KeyError(f"Parameter '{parameter_name}' not found in defaults")
        
        return defaults[parameter_name]
    
    def has_parameter(self, parameter_name: str) -> bool:
        """
        Check if a parameter is configured.
        
        Args:
            parameter_name: Name of the parameter to check
            
        Returns:
            bool: True if parameter exists in configuration
        """
        return parameter_name in self.get_parameter_names()
    
    def get_external_call_config(self, service_name: str) -> Optional[Dict[str, Any]]:
        """
        Get configuration for a specific external service.
        
        Args:
            service_name: Name of the external service
            
        Returns:
            Optional[Dict[str, Any]]: Service configuration or None if not found
        """
        return self.external_call_data.get(service_name)
    
    def get_all_external_services(self) -> List[str]:
        """
        Get list of all configured external services.
        
        Returns:
            List[str]: List of external service names
        """
        return list(self.external_call_data.keys())
    
    def has_external_service(self, service_name: str) -> bool:
        """
        Check if an external service is configured.
        
        Args:
            service_name: Name of the service to check
            
        Returns:
            bool: True if service is configured
        """
        return service_name in self.external_call_data
    
    def validate_parameter_values(self, parameters: Dict[str, Any]) -> Dict[str, str]:
        """
        Validate parameter values against configured names and types.
        
        Args:
            parameters: Parameter values to validate
            
        Returns:
            Dict[str, str]: Validation errors (empty if all valid)
        """
        errors = {}
        configured_names = self.get_parameter_names()
        
        # Check for unknown parameters
        for param_name in parameters.keys():
            if param_name not in configured_names:
                errors[param_name] = f"Unknown parameter '{param_name}'"
        
        # Add any additional validation logic here based on your requirements
        # For example, type checking, range validation, etc.
        
        return errors
    
    def merge_with_defaults(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge provided parameters with default values.
        
        Args:
            parameters: Parameter values to merge
            
        Returns:
            Dict[str, Any]: Merged parameters (defaults + provided values)
        """
        defaults = self.get_parameter_defaults()
        merged = defaults.copy()
        merged.update(parameters)
        
        self.logger.info(f"Merged {len(parameters)} provided parameters with {len(defaults)} defaults")
        return merged
    
    def get_algorithm_config(self, algorithm_name: str) -> Dict[str, Any]:
        """
        Get complete configuration for a specific algorithm.
        
        Args:
            algorithm_name: Name of the algorithm
            
        Returns:
            Dict[str, Any]: Algorithm configuration including defaults
        """
        # Start with general defaults
        config = self.get_parameter_defaults()
        
        # Add algorithm-specific defaults if available
        algorithm_defaults = self.get_algorithm_defaults()
        if algorithm_name in algorithm_defaults:
            config.update(algorithm_defaults[algorithm_name])
        
        self.logger.info(f"Retrieved configuration for algorithm: {algorithm_name}")
        return config
    
    def export_config_summary(self) -> Dict[str, Any]:
        """
        Export a summary of the parameters configuration for debugging/logging.
        
        Returns:
            Dict[str, Any]: Configuration summary
        """
        summary = {
            "parameter_count": len(self.get_parameter_names()),
            "parameter_names": self.get_parameter_names(),
            "defaults_count": len(self.get_parameter_defaults()),
            "external_services_count": len(self.get_all_external_services()),
            "external_services": self.get_all_external_services(),
            "has_algorithm_defaults": bool(self.get_algorithm_defaults())
        }
        
        return summary
    
    def __repr__(self) -> str:
        """String representation of ParametersConfig."""
        param_count = len(self.get_parameter_names())
        service_count = len(self.get_all_external_services())
        return f"ParametersConfig(parameters={param_count}, external_services={service_count})"