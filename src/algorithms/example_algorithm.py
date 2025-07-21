"""Example algorithm implementation for the my_new_project project."""

import logging
from typing import Dict, Any, Optional, List, Union
import pandas as pd
from datetime import datetime

# Import base algorithm class
from base_data_project.algorithms.base import BaseAlgorithm

# Import project-specific components
from src.config import PROJECT_NAME

# Set up logger
logger = logging.getLogger(PROJECT_NAME)

class ExampleAlgorithm(BaseAlgorithm):
    """
    Example algorithm implementation demonstrating the standard algorithm structure.
    
    This algorithm implements a simple data analysis process:
    1. Adapt data: Prepare input data for processing
    2. Execute algorithm: Perform the core processing logic
    3. Format results: Structure the outputs consistently
    
    The algorithm calculates basic statistics on numerical data and classifies
    items based on configurable thresholds.
    """

    def __init__(self, algo_name: str = "ExampleAlgorithm", parameters: Optional[Dict[str, Any]] = None):
        """
        Initialize the algorithm with name and parameters.
        
        Args:
            algo_name: Name of the algorithm
            parameters: Optional dictionary of algorithm parameters
        """
        # Define default parameters
        default_params = {
            'threshold': 50.0,           # Threshold for classification
            'include_outliers': False,   # Whether to include outliers in analysis
            'outlier_threshold': 2.0     # Standard deviations for outlier detection
        }
        
        # Update defaults with any provided parameters
        if parameters:
            default_params.update(parameters)
        
        # Initialize the parent class with algorithm name and parameters
        super().__init__(algo_name=algo_name, parameters=default_params)
        
        # Add any algorithm-specific initialization
        self.logger.info(f"Initialized {self.algo_name} with parameters: {self.parameters}")

    def adapt_data(self, data: Any = None) -> Any:
        """
        Transform input data into algorithm-specific format.
        
        This method prepares the input data for processing, handling different
        input formats and structures.
        
        Args:
            data: Input data (could be DataFrame, dictionary, etc.)
            
        Returns:
            Transformed data ready for algorithm processing
        """
        self.logger.info(f"Adapting data for {self.algo_name}")
        
        try:
            # Handle different input types
            if data is None:
                self.logger.warning("No data provided")
                return None
                
            # Create a standardized data structure for processing
            adapted_data = {}
            
            # Case 1: Input is a dictionary of entities
            if isinstance(data, dict):
                self.logger.info("Processing dictionary input with multiple entities")
                
                # Process each entity in the dictionary
                for entity_name, entity_data in data.items():
                    # Convert to DataFrame if not already
                    if isinstance(entity_data, pd.DataFrame):
                        df = entity_data.copy()
                    else:
                        # Try to convert to DataFrame
                        try:
                            df = pd.DataFrame(entity_data)
                        except Exception as e:
                            self.logger.warning(f"Could not convert {entity_name} to DataFrame: {str(e)}")
                            continue
                    
                    # Extract numerical columns for analysis
                    numerical_cols = df.select_dtypes(include=['number']).columns.tolist()
                    
                    if not numerical_cols:
                        self.logger.warning(f"No numerical columns found in {entity_name}")
                        # Store the original data anyway
                        adapted_data[entity_name] = {
                            'data': df,
                            'numerical_cols': [],
                            'categorical_cols': df.select_dtypes(include=['object']).columns.tolist()
                        }
                    else:
                        # Store adapted entity data
                        adapted_data[entity_name] = {
                            'data': df,
                            'numerical_cols': numerical_cols,
                            'categorical_cols': df.select_dtypes(include=['object']).columns.tolist()
                        }
                        
                        self.logger.info(f"Adapted {entity_name}: {len(df)} rows, {len(numerical_cols)} numerical columns")
            
            # Case 2: Input is a single DataFrame
            elif isinstance(data, pd.DataFrame):
                self.logger.info("Processing single DataFrame input")
                
                df = data.copy()
                numerical_cols = df.select_dtypes(include=['number']).columns.tolist()
                
                if not numerical_cols:
                    self.logger.warning("No numerical columns found in DataFrame")
                
                # Store as a single entity
                adapted_data['main'] = {
                    'data': df,
                    'numerical_cols': numerical_cols,
                    'categorical_cols': df.select_dtypes(include=['object']).columns.tolist()
                }
                
                self.logger.info(f"Adapted DataFrame: {len(df)} rows, {len(numerical_cols)} numerical columns")
            
            # Case 3: Input is a list of records
            elif isinstance(data, list) and len(data) > 0:
                self.logger.info("Processing list input")
                
                # Try to convert to DataFrame
                try:
                    df = pd.DataFrame(data)
                    numerical_cols = df.select_dtypes(include=['number']).columns.tolist()
                    
                    # Store as a single entity
                    adapted_data['main'] = {
                        'data': df,
                        'numerical_cols': numerical_cols,
                        'categorical_cols': df.select_dtypes(include=['object']).columns.tolist()
                    }
                    
                    self.logger.info(f"Adapted list: {len(df)} rows, {len(numerical_cols)} numerical columns")
                except Exception as e:
                    self.logger.warning(f"Could not convert list to DataFrame: {str(e)}")
                    return None
            
            # Case 4: Unsupported input type
            else:
                self.logger.warning(f"Unsupported input type: {type(data)}")
                return None
            
            # Check if any data was adapted
            if not adapted_data:
                self.logger.warning("No data was successfully adapted")
                return None
            
            self.logger.info(f"Data adaptation complete: {len(adapted_data)} entities")
            return adapted_data
            
        except Exception as e:
            self.logger.error(f"Error during data adaptation: {str(e)}", exc_info=True)
            return None

    def execute_algorithm(self, adapted_data: Any = None) -> Dict[str, Any]:
        """
        Execute the core algorithm logic.
        
        This method implements the actual analysis logic using the adapted data.
        
        Args:
            adapted_data: Data prepared by adapt_data
            
        Returns:
            Dictionary with algorithm results
        """
        self.logger.info(f"Executing algorithm {self.algo_name}")
        
        try:
            # Check if data is available
            if not adapted_data:
                self.logger.warning("No adapted data available for execution")
                return {
                    "status": "failed",
                    "error": "No data available for processing"
                }
            
            # Get algorithm parameters
            threshold = self.parameters.get('threshold', 50.0)
            include_outliers = self.parameters.get('include_outliers', False)
            outlier_threshold = self.parameters.get('outlier_threshold', 2.0)
            
            self.logger.info(f"Using parameters: threshold={threshold}, include_outliers={include_outliers}")
            
            # Initialize results structure
            results = {
                "entity_results": {},
                "overall_metrics": {
                    "total_entities": len(adapted_data),
                    "total_records": 0,
                    "total_numerical_columns": 0,
                    "entities_above_threshold": 0
                }
            }
            
            # Process each entity
            for entity_name, entity_data in adapted_data.items():
                self.logger.info(f"Processing entity: {entity_name}")
                
                df = entity_data['data']
                numerical_cols = entity_data['numerical_cols']
                
                # Skip if no numerical columns
                if not numerical_cols:
                    self.logger.warning(f"No numerical columns in {entity_name}, skipping analysis")
                    results["entity_results"][entity_name] = {
                        "status": "skipped",
                        "reason": "No numerical columns",
                        "record_count": len(df)
                    }
                    continue
                
                # Initialize entity results
                entity_results = {
                    "status": "completed",
                    "record_count": len(df),
                    "numerical_columns": len(numerical_cols),
                    "column_statistics": {},
                    "classifications": {},
                    "above_threshold": False
                }
                
                # Calculate statistics for each numerical column
                for column in numerical_cols:
                    # Get column data
                    column_data = df[column].dropna()
                    
                    # Handle outliers if requested
                    if not include_outliers:
                        mean = column_data.mean()
                        std = column_data.std()
                        lower_bound = mean - (outlier_threshold * std)
                        upper_bound = mean + (outlier_threshold * std)
                        
                        # Filter outliers
                        filtered_data = column_data[(column_data >= lower_bound) & (column_data <= upper_bound)]
                        outlier_count = len(column_data) - len(filtered_data)
                        
                        if outlier_count > 0:
                            self.logger.info(f"Removed {outlier_count} outliers from {entity_name}.{column}")
                            column_data = filtered_data
                    
                    # Calculate statistics
                    stats = {
                        "count": len(column_data),
                        "mean": float(column_data.mean()) if len(column_data) > 0 else 0,
                        "median": float(column_data.median()) if len(column_data) > 0 else 0,
                        "std": float(column_data.std()) if len(column_data) > 0 else 0,
                        "min": float(column_data.min()) if len(column_data) > 0 else 0,
                        "max": float(column_data.max()) if len(column_data) > 0 else 0
                    }
                    
                    # Classify column based on mean value
                    classification = "high" if stats["mean"] > threshold else "low"
                    
                    # Store column results
                    entity_results["column_statistics"][column] = stats
                    entity_results["classifications"][column] = classification
                    
                    # Check if any column is above threshold
                    if stats["mean"] > threshold:
                        entity_results["above_threshold"] = True
                
                # Store entity results
                results["entity_results"][entity_name] = entity_results
                
                # Update overall metrics
                results["overall_metrics"]["total_records"] += entity_results["record_count"]
                results["overall_metrics"]["total_numerical_columns"] += entity_results["numerical_columns"]
                
                if entity_results["above_threshold"]:
                    results["overall_metrics"]["entities_above_threshold"] += 1
            
            # Calculate overall statistics
            total_entities = results["overall_metrics"]["total_entities"]
            entities_above_threshold = results["overall_metrics"]["entities_above_threshold"]
            
            results["overall_metrics"]["percent_above_threshold"] = (
                (entities_above_threshold / total_entities) * 100
                if total_entities > 0 else 0
            )
            
            self.logger.info(f"Algorithm execution complete: processed {total_entities} entities")
            return results
            
        except Exception as e:
            self.logger.error(f"Error during algorithm execution: {str(e)}", exc_info=True)
            return {
                "status": "failed",
                "error": str(e)
            }

    def format_results(self, algorithm_results: pd.DataFrame = pd.DataFrame()) -> Dict[str, Any]:
        """
        Format the algorithm results for output.
        
        Args:
            algorithm_results: Results from execute_algorithm
            
        Returns:
            Dictionary with formatted results
        """
        self.logger.info(f"Formatting results for {self.algo_name}")

        # TODO: implement the logic
        return {}

