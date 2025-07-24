"""
File and directory paths configuration management.

This module handles path configuration for data sources based on the database usage setting:
- When use_db=True: Loads SQL query file paths
- When use_db=False: Loads CSV file paths

Configuration is loaded from:
- src/settings/folder_hierarchy.json (directory structure)
- src/settings/sql_filepaths.json (SQL query paths, when use_db=True)
- CSV configuration (when use_db=False)
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional
from base_data_project.log_config import get_logger


class PathsConfig:
    """
    File and directory paths configuration management.
    
    This class manages file system paths for data access, with different
    configurations based on whether the system uses database or CSV files.
    
    When use_db=True:
        sql_processing_paths: Dict[str, str] - SQL files for processing data
        sql_auxiliary_paths: Dict[str, str] - SQL files for auxiliary data
        sql_raw_paths: Dict[str, str] - SQL files for raw data access
        
    When use_db=False:
        csv_filepaths: Dict[str, str] - Full paths to CSV data files
        
    Always available:
        project_root_dir: str - Root directory of the project
        use_db: bool - Database usage flag
    """
    
    def __init__(self, project_root_dir: str, use_db: bool, project_name: str):
        """
        Initialize paths configuration.
        
        Args:
            project_root_dir: Root directory path for the project
            use_db: Whether to use database (True) or CSV files (False)
            project_name: Project name for logging purposes
            
        Raises:
            FileNotFoundError: If required configuration files are missing
            ValueError: If configuration validation fails
            json.JSONDecodeError: If JSON files are malformed
        """
        self.logger = get_logger(project_name)
        self.project_root_dir = project_root_dir
        self.use_db = use_db
        
        # Load base path hierarchy
        self._path_hierarchy = self._load_path_hierarchy()
        self._validate_path_hierarchy()
        
        # Initialize path dictionaries
        self.sql_processing_paths: Dict[str, str] = {}
        self.sql_auxiliary_paths: Dict[str, str] = {}
        self.sql_raw_paths: Dict[str, str] = {}
        self.csv_filepaths: Dict[str, str] = {}
        
        # Load appropriate path configurations based on use_db
        if use_db:
            self.logger.info("Loading SQL-based path configuration")
            self._load_sql_paths()
        else:
            self.logger.info("Loading CSV-based path configuration")
            self._load_csv_paths()
        
        self.logger.info(f"Paths configuration loaded (use_db={use_db})")
    
    def _load_path_hierarchy(self) -> Dict[str, Any]:
        """
        Load the folder hierarchy configuration.
        
        Returns:
            Dict[str, Any]: Folder hierarchy structure
            
        Raises:
            FileNotFoundError: If folder_hierarchy.json is missing
            json.JSONDecodeError: If JSON is malformed
        """
        try:
            with open("src/settings/folder_hierarchy.json", "r") as file:
                hierarchy = json.load(file)
            self.logger.info("Path hierarchy loaded successfully")
            return hierarchy
        except FileNotFoundError:
            raise FileNotFoundError("Path hierarchy file 'folder_hierarchy.json' not found")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in path hierarchy: {e}")
    
    def _validate_path_hierarchy(self) -> None:
        """
        Validate that the path hierarchy is correctly structured.
        
        Raises:
            ValueError: If hierarchy structure is invalid
        """
        if not isinstance(self._path_hierarchy, dict):
            raise ValueError("Path hierarchy must be a dictionary")
        
        # Add specific validation logic based on your folder_hierarchy.json structure
        # For example, check for required top-level directories
        self.logger.info("Path hierarchy validation passed")
    
    def _load_sql_paths(self) -> None:
        """
        Load SQL-based path configurations.
        
        Raises:
            FileNotFoundError: If sql_filepaths.json is missing
            ValueError: If SQL paths configuration is invalid
            json.JSONDecodeError: If JSON is malformed
        """
        try:
            with open("src/settings/sql_filepaths.json", "r") as file:
                sql_config = json.load(file)
            
            # Extract different types of SQL paths
            self.sql_processing_paths = sql_config.get("sql_processing_paths", {})
            self.sql_auxiliary_paths = sql_config.get("sql_auxiliary_paths", {})
            self.sql_raw_paths = sql_config.get("available_entities_raw", {})
            
            # Build full paths for SQL files
            self._build_sql_file_paths()
            
            self._validate_sql_paths()
            self.logger.info("SQL paths loaded successfully")
            
        except FileNotFoundError:
            raise FileNotFoundError("SQL filepaths file 'sql_filepaths.json' not found")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in SQL filepaths: {e}")
    
    def _build_sql_file_paths(self) -> None:
        """
        Build full file paths for SQL files based on project root directory.
        """
        # Build full paths for processing SQL files
        for entity, relative_path in self.sql_processing_paths.items():
            if relative_path:
                full_path = os.path.join(self.project_root_dir, relative_path)
                self.sql_processing_paths[entity] = full_path
        
        # Build full paths for auxiliary SQL files
        for entity, relative_path in self.sql_auxiliary_paths.items():
            if relative_path:
                full_path = os.path.join(self.project_root_dir, relative_path)
                self.sql_auxiliary_paths[entity] = full_path
        
        # Build full paths for raw SQL files
        for entity, relative_path in self.sql_raw_paths.items():
            if relative_path:
                full_path = os.path.join(self.project_root_dir, relative_path)
                self.sql_raw_paths[entity] = full_path
    
    def _validate_sql_paths(self) -> None:
        """
        Validate SQL path configurations.
        
        Raises:
            ValueError: If SQL paths are invalid
        """
        if not isinstance(self.sql_processing_paths, dict):
            raise ValueError("sql_processing_paths must be a dictionary")
        
        if not isinstance(self.sql_auxiliary_paths, dict):
            raise ValueError("sql_auxiliary_paths must be a dictionary")
            
        if not isinstance(self.sql_raw_paths, dict):
            raise ValueError("sql_raw_paths must be a dictionary")
        
        self.logger.info("SQL paths validation passed")
    
    def _load_csv_paths(self) -> None:
        """
        Load CSV-based path configurations.
        
        Note: This method assumes CSV configuration exists. Adjust the file name
        and structure based on your actual CSV configuration setup.
        
        Raises:
            FileNotFoundError: If CSV configuration is missing
            ValueError: If CSV paths configuration is invalid
            json.JSONDecodeError: If JSON is malformed
        """
        try:
            # Try to load from a dedicated CSV config file first
            csv_config_file = "src/settings/csv_filenames.json"
            
            if os.path.exists(csv_config_file):
                with open(csv_config_file, "r") as file:
                    csv_config = json.load(file)
            else:
                # Fallback: Extract CSV paths from folder hierarchy
                csv_config = self._extract_csv_paths_from_hierarchy()
            
            # Build full paths for CSV files
            self.csv_filepaths = {}
            for entity, filename in csv_config.items():
                if filename:
                    # Assume CSV files are in data directory
                    full_path = os.path.join(self.project_root_dir, "data", filename)
                    self.csv_filepaths[entity] = full_path
            
            self._validate_csv_paths()
            self.logger.info("CSV paths loaded successfully")
            
        except FileNotFoundError:
            # If no CSV config found, create empty configuration
            self.logger.warning("No CSV configuration found, using empty CSV paths")
            self.csv_filepaths = {}
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in CSV configuration: {e}")
    
    def _extract_csv_paths_from_hierarchy(self) -> Dict[str, str]:
        """
        Extract CSV file paths from the folder hierarchy when no dedicated CSV config exists.
        
        Returns:
            Dict[str, str]: Entity name to CSV filename mapping
        """
        # This is a fallback method - implement based on your folder_hierarchy.json structure
        # For now, return empty dict - you can customize this based on your needs
        self.logger.info("Extracting CSV paths from folder hierarchy")
        return {}
    
    def _validate_csv_paths(self) -> None:
        """
        Validate CSV path configurations.
        
        Raises:
            ValueError: If CSV paths are invalid
        """
        if not isinstance(self.csv_filepaths, dict):
            raise ValueError("csv_filepaths must be a dictionary")
        
        self.logger.info("CSV paths validation passed")
    
    def get_processing_paths(self) -> Dict[str, str]:
        """
        Get processing data paths (SQL or CSV based on configuration).
        
        Returns:
            Dict[str, str]: Processing data paths
        """
        if self.use_db:
            return self.sql_processing_paths.copy()
        else:
            # For CSV mode, return all CSV paths (you might want to filter these)
            return self.csv_filepaths.copy()
    
    def get_auxiliary_paths(self) -> Dict[str, str]:
        """
        Get auxiliary data paths (SQL only, empty for CSV mode).
        
        Returns:
            Dict[str, str]: Auxiliary data paths
        """
        if self.use_db:
            return self.sql_auxiliary_paths.copy()
        else:
            return {}
    
    def get_raw_paths(self) -> Dict[str, str]:
        """
        Get raw data paths (SQL only, empty for CSV mode).
        
        Returns:
            Dict[str, str]: Raw data paths
        """
        if self.use_db:
            return self.sql_raw_paths.copy()
        else:
            return {}
    
    def get_all_paths(self) -> Dict[str, Any]:
        """
        Get all configured paths for the current mode.
        
        Returns:
            Dict[str, Any]: All paths organized by category
        """
        if self.use_db:
            return {
                "sql_processing_paths": self.sql_processing_paths,
                "sql_auxiliary_paths": self.sql_auxiliary_paths,
                "sql_raw_paths": self.sql_raw_paths
            }
        else:
            return {
                "csv_filepaths": self.csv_filepaths
            }
    
    def validate_file_existence(self) -> Dict[str, bool]:
        """
        Validate that configured files actually exist on the file system.
        
        Returns:
            Dict[str, bool]: File existence status for each configured path
        """
        existence_status = {}
        
        if self.use_db:
            # Check SQL files
            all_sql_paths = {
                **self.sql_processing_paths,
                **self.sql_auxiliary_paths,
                **self.sql_raw_paths
            }
            
            for entity, file_path in all_sql_paths.items():
                existence_status[f"sql_{entity}"] = os.path.exists(file_path)
        else:
            # Check CSV files
            for entity, file_path in self.csv_filepaths.items():
                existence_status[f"csv_{entity}"] = os.path.exists(file_path)
        
        return existence_status
    
    def __repr__(self) -> str:
        """String representation of PathsConfig."""
        mode = "SQL" if self.use_db else "CSV"
        path_count = len(self.get_all_paths())
        return f"PathsConfig(mode='{mode}', paths_configured={path_count})"