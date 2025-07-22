"""File containing the path configuration class"""

# Dependencies
import json
import os
from pathlib import Path

# Local stuff
from base_data_project.log_config import get_logger

project_name = "algoritmo_GD"

class PathConfig:
    """Class used to manage the paths needed for the project"""

    def __init__(self, project_root_dir: str, use_db: bool = True):
        """Initialize the path configuration"""
        self.logger = get_logger(project_name)

        # Create empty dicts
        self.csv_filepaths_dict = {}
        self.sql_filepaths_dict = {}

        # Load the path configuration
        self.path_config_dict = self.load_path_config_json()

        # Validate the path configuration
        if not self.validate_path_config():
            raise ValueError("Path configuration validation failed") 

        # If use_db is True, load the sql filepaths
        if use_db:
            self.logger.info(f"Using database to load the filepaths. Creating empty csv_filepaths_dict")
            self.csv_filepaths_dict = {}
            
            # Load the sql queries filepaths
            sql_queries_dict = self.load_sql_queries_filepaths()
            
            # Validate the sql queries filepaths
            if not self.validate_sql_queries_filepaths(sql_queries_dict, self.path_config_dict):
                raise ValueError("SQL queries filepaths validation failed")
            
            # Construct the sql queries filepaths
            self.sql_filepaths_dict = self.sql_queries_filepaths_constructor(sql_queries_dict, project_root_dir)
        else:
            # Load the csv filepaths
            csv_filenames_dict = self.load_csv_filepaths()

            # Validate the csv filepaths
            if not self.validate_csv_filepaths(csv_filenames_dict, self.path_config_dict):
                raise ValueError("CSV filepaths validation failed")

            # Construct the csv filepaths
            self.csv_filepaths_dict = self.csv_filepaths_constructor(csv_filenames_dict, project_root_dir)
            
            # Create empty sql filepaths dict since not using database
            self.sql_filepaths_dict = {}

    # Property accessors to provide same interface as CONFIG
    @property
    def available_entities_processing(self) -> dict:
        """
        Access processing entities with same interface as CONFIG['available_entities_processing']
        Maps to sql_processing_paths when using database
        """
        if hasattr(self, 'sql_filepaths_dict') and self.sql_filepaths_dict:
            return self.sql_filepaths_dict.get("sql_processing_paths", {})
        return {}

    @property
    def available_entities_aux(self) -> dict:
        """
        Access auxiliary entities with same interface as CONFIG['available_entities_aux']
        Maps to sql_auxiliary_paths when using database
        """
        if hasattr(self, 'sql_filepaths_dict') and self.sql_filepaths_dict:
            return self.sql_filepaths_dict.get("sql_auxiliary_paths", {})
        return {}

    @property
    def available_entities_raw(self) -> dict:
        """
        Access raw entities with same interface as CONFIG['available_entities_raw']
        Maps to available_entities_raw in SQL config
        """
        if hasattr(self, 'sql_filepaths_dict') and self.sql_filepaths_dict:
            return self.sql_filepaths_dict.get("available_entities_raw", {})
        return {}

    @property
    def dummy_data_filepaths(self) -> dict:
        """
        Access CSV file paths with same interface as CONFIG['dummy_data_filepaths']
        Maps to csv_filepaths_dict when using CSV files
        """
        if hasattr(self, 'csv_filepaths_dict') and self.csv_filepaths_dict:
            return self.csv_filepaths_dict
        return {}

    def load_path_config_json(self) -> dict:
        """Load the path configuration"""
        # Try to load the path configuration
        try:
            with open("src/settings/folder_hierarchy.json", "r") as file:
                path_config_dict = json.load(file)
            self.logger.info(f"Path configuration loaded successfully")
            return path_config_dict
        
        # If the file is not found, raise an error
        except FileNotFoundError:
            self.logger.error(f"Path configuration file not found")
            raise FileNotFoundError(f"Path configuration file not found")
        
        except Exception as e:
            self.logger.error(f"Error loading path configuration: {e}")
            raise e

    def validate_path_config(self) -> bool:
        """Validate that all paths in the configuration correspond to existing folders"""
        missing_paths = []
        
        def _validate_recursive(config_dict: dict, current_path: str = ""):
            """Recursively validate paths in the configuration dictionary"""
            for key, value in config_dict.items():
                # Build the full path
                if current_path:
                    full_path = os.path.join(current_path, key)
                else:
                    full_path = key
                
                # Check if the path exists
                if not Path(full_path).exists():
                    missing_paths.append(full_path)
                    self.logger.warning(f"Path does not exist: {full_path}")
                else:
                    self.logger.debug(f"Path exists: {full_path}")
                
                # If value is a dictionary, recurse into it
                if isinstance(value, dict):
                    _validate_recursive(value, full_path)
        
        # Start validation from the root
        _validate_recursive(self.path_config_dict)
        
        # Log results
        if missing_paths:
            self.logger.error(f"Validation failed. Missing paths: {missing_paths}")
            return False
        else:
            self.logger.info("Path configuration validation successful - all paths exist")
            return True

    def load_csv_filepaths(self):
        """Load the dummy data filepaths"""
        try:
            # Load the csv filepaths
            with open("src/settings/csv_filepaths.json", "r") as file:
                csv_filenames_dict = json.load(file)
            self.logger.info(f"CSV filepaths loaded successfully")
            return csv_filenames_dict
        except FileNotFoundError:
            self.logger.error(f"CSV filepaths file not found")
            raise FileNotFoundError(f"CSV filepaths file not found")
        except Exception as e:
            self.logger.error(f"Error loading csv filepaths: {e}")
            raise e

    def validate_csv_filepaths(self, csv_filenames_dict: dict, path_config_dict: dict) -> bool:
        """Validate the csv filepaths"""
        # TODO: Implement the validation function for the csv filepaths
        try:
            # Validate the csv filepaths
            if not csv_filenames_dict:
                self.logger.error(f"CSV filepaths dictionary is empty")
                return False
            
            # Validate the hierarchy
            if not csv_filenames_dict.get("hierarchy"):
                self.logger.error(f"CSV filepaths dictionary does not contain a hierarchy")
                return False

            # Validate the entities
            if not csv_filenames_dict.get("entities"):
                self.logger.error(f"CSV filepaths dictionary does not contain any entities")
                return False

            if csv_filenames_dict["hierarchy"][0] not in path_config_dict.keys():
                self.logger.error(f"CSV filepaths dictionary does not contain a data folder")
                return False

            if csv_filenames_dict["hierarchy"][1] not in path_config_dict[csv_filenames_dict["hierarchy"][0]].keys():
                self.logger.error(f"CSV filepaths dictionary does not contain a csvs folder")
                return False

            self.csv_filenames_dict = csv_filenames_dict

            return True
        except Exception as e:
            self.logger.error(f"Error validating csv filepaths: {e}")
            raise e

    def csv_filepaths_constructor(self, csv_filenames_dict: dict, project_root_dir: str) -> dict:
        """Construct the csv filepaths"""
        try: 
            csv_filepaths_dict = {}
            
            # Build the hierarchy path progressively
            current_path = project_root_dir
            for folder in csv_filenames_dict["hierarchy"]:
                current_path = os.path.join(current_path, folder)

            # Construct full absolute paths for each entity
            missing_files = []
            for entity, filename in csv_filenames_dict["entities"].items():
                full_path = os.path.join(current_path, filename)
                csv_filepaths_dict[entity] = full_path
                
                # Validate that the CSV file exists
                if not Path(full_path).exists():
                    missing_files.append(f"{entity}: {full_path}")
                    self.logger.warning(f"CSV file does not exist: {entity} -> {full_path}")

            # Log results
            if missing_files:
                self.logger.error(f"Some CSV files are missing: {missing_files}")
                raise FileNotFoundError(f"Missing CSV files: {missing_files}")
            
            self.logger.info(f"CSV filepaths constructed successfully: {len(csv_filepaths_dict)} entities")
            return csv_filepaths_dict
                
        except Exception as e:
            self.logger.error(f"Error constructing csv filepaths: {e}")
            raise e

    def load_sql_queries_filepaths(self):
        """Load the sql queries filepaths"""
        try:
            with open("src/settings/sql_filepaths.json", "r") as file:
                sql_queries_dict = json.load(file)
            self.logger.info(f"SQL queries filepaths loaded successfully")
            return sql_queries_dict
        except FileNotFoundError:
            self.logger.error(f"SQL queries filepaths file not found")
            raise FileNotFoundError(f"SQL queries filepaths file not found")
        except Exception as e:
            self.logger.error(f"Error loading sql queries filepaths: {e}")
            raise e

    def validate_sql_queries_filepaths(self, sql_queries_dict: dict, path_config_dict: dict) -> bool:
        """Validate the sql queries filepaths"""
        try:
            # Validate the sql queries filepaths
            if not sql_queries_dict:
                self.logger.error(f"SQL queries filepaths dictionary is empty")
                return False
            
            # Validate the hierarchy
            if not sql_queries_dict.get("hierarchy"):
                self.logger.error(f"SQL queries filepaths dictionary does not contain a hierarchy")
                return False

            # Validate at least one category exists (excluding hierarchy)
            categories = [key for key in sql_queries_dict.keys() if key != "hierarchy"]
            if not categories:
                self.logger.error(f"SQL queries filepaths dictionary does not contain any query categories")
                return False

            # Validate hierarchy path exists in folder structure
            current_level = path_config_dict
            for folder in sql_queries_dict["hierarchy"]:
                if folder not in current_level:
                    self.logger.error(f"SQL queries hierarchy folder '{folder}' not found in path configuration")
                    return False
                current_level = current_level[folder]

            self.logger.info("SQL queries filepaths validation successful")
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating sql queries filepaths: {e}")
            raise e

    def sql_queries_filepaths_constructor(self, sql_queries_dict: dict, project_root_dir: str) -> dict:
        """Construct the sql queries filepaths"""
        try:
            # Initialize dictionary with all categories (excluding hierarchy)
            sql_filepaths_dict = {}
            for category in sql_queries_dict.keys():
                if category != "hierarchy":
                    sql_filepaths_dict[category] = {}
            
            # Build the hierarchy path progressively
            current_path = project_root_dir
            for folder in sql_queries_dict["hierarchy"]:
                current_path = os.path.join(current_path, folder)

            # Construct full absolute paths for all query categories
            missing_files = []
            for category in sql_queries_dict.keys():
                if category != "hierarchy":
                    for entity, filename in sql_queries_dict[category].items():
                        # Skip empty filenames
                        if not filename:
                            self.logger.info(f"Skipping empty filename for {category}.{entity}")
                            continue
                            
                        full_path = os.path.join(current_path, filename)
                        sql_filepaths_dict[category][entity] = full_path
                        
                        # Validate that the SQL file exists
                        if not Path(full_path).exists():
                            missing_files.append(f"{category}.{entity}: {full_path}")
                            self.logger.warning(f"SQL file does not exist: {category}.{entity} -> {full_path}")

            # Log results
            if missing_files:
                self.logger.error(f"Some SQL files are missing: {missing_files}")
                raise FileNotFoundError(f"Missing SQL files: {missing_files}")
            
            total_files = sum(len(category_dict) for category_dict in sql_filepaths_dict.values())
            self.logger.info(f"SQL queries filepaths constructed successfully: {total_files} queries")
            return sql_filepaths_dict
                
        except Exception as e:
            self.logger.error(f"Error constructing sql queries filepaths: {e}")
            raise e
