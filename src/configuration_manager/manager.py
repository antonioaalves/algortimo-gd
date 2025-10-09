"""
Main Configuration Manager - orchestrates all config loading.

This module provides the primary ConfigurationManager class that coordinates
the loading and initialization of all configuration components.
"""

from typing import Optional, Dict, Any
from base_data_project.log_config import get_logger

from .system_config import SystemConfig
from .database_config import DatabaseConfig
from .paths_config import PathsConfig
from .parameters_config import ParametersConfig
from .algorithm_config import AlgorithmConfig
from .stages_config import StagesConfig


class ConfigurationManager:
    """
    Main configuration manager - industry standard implementation.
    
    This class orchestrates the loading of all configuration components
    with proper lifecycle management and dependency resolution.
    
    Architecture:
        1. SystemConfig loads first (establishes project name and core settings)
        2. Other configs load with proper project name for logging
        3. Database config is optional based on system.use_db flag
        4. All configs perform strict validation during initialization
    
    Access Patterns:
        config.system.project_name          # System settings
        config.database.host                # Database settings (if enabled)
        config.paths.csv_filepaths          # File paths (mode-dependent)
        config.parameters.process_parameters # Process parameters
        config.stages.stages                # Workflow stages
    
    Properties:
        project_name: str - Quick access to project name
        is_database_enabled: bool - Check if database is configured
    """
    
    def __init__(self):
        """
        Initialize the configuration manager with proper lifecycle management.
        
        Initialization phases:
        1. Load system config (establishes project name and logging foundation)
        2. Load database config (conditional on system.use_db setting)
        3. Load paths config (mode-dependent: SQL vs CSV)
        4. Load parameters config (process parameters and external calls)
        5. Load stages config (workflow definitions)
        6. Perform final validation
        
        Raises:
            FileNotFoundError: If any required configuration file is missing
            ValueError: If any configuration validation fails
            json.JSONDecodeError: If any JSON configuration file is malformed
        """
        # Phase 1: Load system config (establishes project name and logging)
        self.system = SystemConfig()
        
        # Phase 2: Load other configs with proper project name for logging
        project_name = self.system.project_name
        self.logger = get_logger(project_name)
        # Load database config only if needed
        self.database: Optional[DatabaseConfig] = None
        if self.system.use_db:
            self.database = DatabaseConfig(
                environment=self.system.environment,
                project_name=project_name
            )
            self.logger.info("Database config loaded successfully")
            self.logger.info(f"Database config: {self.database}")
            self.logger.info(f"Database config: {self.database.get_connection_url()}")
        
        # Load paths config (behavior depends on use_db setting)
        self.paths = PathsConfig(
            project_root_dir=self.system.project_root_dir,
            use_db=self.system.use_db,
            project_name=project_name
        )
        
        # Load parameters config
        self.parameters = ParametersConfig(project_name=project_name)
        
        # Load algorithm component config (algorithm/restrictions/solver)
        self.algorithm = AlgorithmConfig(project_name=project_name)

        # Load stages config
        self.stages = StagesConfig(project_name=project_name)
        
        # Initialize main logger and log completion
        self.logger = get_logger(project_name)
        self.logger.info("ConfigurationManager initialization completed successfully")
        
        # Perform final validation
        if not self.validate_all_configs():
            raise ValueError("Configuration validation failed during initialization")
    
    @property
    def project_name(self) -> str:
        """
        Quick access to project name.
        
        Returns:
            str: The project name from system configuration
        """
        return self.system.project_name
    
    def get_storage_config(self) -> Dict[str, Any]:
        """
        Get storage configuration for data containers.
        
        Extracts storage-specific settings needed by BaseDataContainer and its subclasses.
        Includes storage_dir, cleanup_policy, and db_url (if database is enabled).
        
        Returns:
            Dict[str, Any]: Storage config with keys: storage_dir, cleanup_policy, db_url (optional)
        """
        storage_config = {
            'storage_dir': self.system.storage_strategy.get('storage_dir', 'data/intermediate'),
            'cleanup_policy': self.system.storage_strategy.get('cleanup_policy', 'keep_latest'),
        }
        
        # Add database URL if database is enabled
        if self.is_database_enabled:
            storage_config['db_url'] = self.get_database_url()
        
        return storage_config
    
    @property
    def is_database_enabled(self) -> bool:
        """
        Check if database is enabled and properly configured.
        
        Returns:
            bool: True if database is enabled and configured
        """
        return self.system.use_db and self.database is not None
    
    def get_database_url(self) -> Optional[str]:
        """
        Get database connection URL if database is enabled.
        
        Returns:
            Optional[str]: Database connection URL or None if database not enabled
        """
        if self.database:
            return self.database.get_connection_url()
        return None
    
    def get_environment(self) -> str:
        """
        Get the current environment setting.
        
        Returns:
            str: Environment name (dev, staging, prod, etc.)
        """
        return self.system.environment
    
    def is_development_mode(self) -> bool:
        """
        Check if running in development mode.
        
        Returns:
            bool: True if in development environment
        """
        return self.system.is_development_environment()
    
    def is_production_mode(self) -> bool:
        """
        Check if running in production mode.
        
        Returns:
            bool: True if in production environment
        """
        return self.system.is_production_environment()
    
    def get_data_mode(self) -> str:
        """
        Get the current data access mode.
        
        Returns:
            str: Either 'database' or 'csv' based on configuration
        """
        return "database" if self.is_database_enabled else "csv"
    
    def validate_all_configs(self) -> bool:
        """
        Run comprehensive validation of all configurations.
        
        This method performs cross-config validation and consistency checks
        beyond the individual validation done during component initialization.
        
        Returns:
            bool: True if all validations pass
        """
        try:
            self.logger.info("Starting comprehensive configuration validation")
            
            # Individual validations are already run during initialization
            # Add cross-config validation here
            
            # Validate database config consistency
            if self.system.use_db and not self.database:
                self.logger.error("System config indicates database usage but database config is missing")
                return False
            
            if not self.system.use_db and self.database:
                self.logger.warning("Database config loaded but system config indicates no database usage")
            
            # Validate algorithm consistency
            system_algorithms = self.system.get_algorithm_list()
            parameter_algorithms = list(self.parameters.get_algorithm_defaults().keys())
            
            # Check if parameter algorithms are subset of system algorithms
            for param_algo in parameter_algorithms:
                if param_algo not in system_algorithms:
                    self.logger.warning(f"Algorithm '{param_algo}' has parameter defaults but is not in system algorithm list")
            
            # Validate stages configuration
            stage_errors = self.stages.validate_stage_sequence()
            if stage_errors:
                self.logger.error(f"Stage validation errors: {stage_errors}")
                return False
            
            # Validate file existence if in CSV mode
            if not self.is_database_enabled:
                file_existence = self.paths.validate_file_existence()
                missing_files = [path for path, exists in file_existence.items() if not exists]
                if missing_files:
                    self.logger.warning(f"Some CSV files are missing: {missing_files}")
                    # Don't fail validation for missing files - they might not exist yet
            
            self.logger.info("All configuration validations passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False
    
    def get_config_summary(self) -> dict:
        """
        Get a comprehensive summary of all configuration settings.
        
        Returns:
            dict: Complete configuration summary for debugging/logging
        """
        summary = {
            "project_name": self.project_name,
            "environment": self.get_environment(),
            "data_mode": self.get_data_mode(),
            "database_enabled": self.is_database_enabled,
            "development_mode": self.is_development_mode(),
            "production_mode": self.is_production_mode(),
            
            # System config summary
            "system": {
                "algorithms_count": len(self.system.get_algorithm_list()),
                "algorithms": self.system.get_algorithm_list(),
                "log_level": self.system.get_log_level(),
                "project_root": self.system.project_root_dir
            },
            
            # Database config summary (if enabled)
            "database": None,
            
            # Paths config summary
            "paths": {
                "mode": "database" if self.is_database_enabled else "csv",
                "configured_paths": len(self.paths.get_all_paths())
            },
            
            # Parameters config summary
            "parameters": self.parameters.export_config_summary(),
            # Algorithm component summary
            "algorithm": self.algorithm.export_config_summary(),
            
            # Stages config summary
            "stages": self.stages.export_workflow_summary()
        }
        
        # Add database summary if enabled
        if self.is_database_enabled:
            summary["database"] = self.database.mask_sensitive_info()
        
        return summary
    
    def export_for_logging(self) -> dict:
        """
        Export configuration in a format suitable for logging (with sensitive data masked).
        
        Returns:
            dict: Configuration summary safe for logging
        """
        return self.get_config_summary()
    
    def reload_configuration(self) -> bool:
        """
        Reload all configuration from files.
        
        Warning: This creates new instances of all config objects.
        Any references to old config objects will become stale.
        
        Returns:
            bool: True if reload successful
        """
        try:
            self.logger.info("Reloading configuration from files")
            
            # Save current project name for logging continuity
            current_project_name = self.project_name
            
            # Reload system config
            self.system = SystemConfig()
            
            # Check if project name changed
            if self.system.project_name != current_project_name:
                self.logger.warning(f"Project name changed during reload: {current_project_name} -> {self.system.project_name}")
            
            # Reload other configs
            project_name = self.system.project_name
            
            if self.system.use_db:
                self.database = DatabaseConfig(
                    environment=self.system.environment,
                    project_name=project_name
                )
            else:
                self.database = None
            
            self.paths = PathsConfig(
                project_root_dir=self.system.project_root_dir,
                use_db=self.system.use_db,
                project_name=project_name
            )
            
            self.parameters = ParametersConfig(project_name=project_name)
            self.algorithm = AlgorithmConfig(project_name=project_name)
            self.stages = StagesConfig(project_name=project_name)
            
            # Validate reloaded configuration
            if not self.validate_all_configs():
                self.logger.error("Configuration validation failed after reload")
                return False
            
            self.logger.info("Configuration reload completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Configuration reload failed: {e}")
            return False
    
    def __repr__(self) -> str:
        """String representation of ConfigurationManager."""
        mode = self.get_data_mode()
        env = self.get_environment()
        return f"ConfigurationManager(project='{self.project_name}', mode='{mode}', env='{env}')"