"""File containing the loading functions for the DescansosDataModel"""

# Dependencies
import pandas as pd
from pathlib import Path
from typing import Optional
from base_data_project.log_config import get_logger

# Local stuff
from src.config import PROJECT_NAME, CONFIG, ROOT_DIR

# Set up logger
logger = get_logger(PROJECT_NAME)

def load_valid_emp_csv() -> pd.DataFrame:
    """
    Load and validate employee data from configured CSV file.
    
    Returns:
        pd.DataFrame: Employee data with validated structure
        
    Raises:
        ValueError: If file path is not configured or file is empty
        FileNotFoundError: If the specified file doesn't exist
        pd.errors.EmptyDataError: If CSV file is empty
        pd.errors.ParserError: If CSV file is malformed
        PermissionError: If file cannot be read due to permissions
    
    Example:
        >>> df = valid_emp()
        >>> print(f"Loaded {len(df)} employee records")
    """
    
    # Get and validate file path from config
    file_path = CONFIG.get('dummy_data_filepaths', {}).get('valid_emp', '')
    
    try:
        # Load CSV with robust parsing options
        df = pd.read_csv(
            file_path,
            encoding='utf-8',
            na_values=['', 'NULL', 'null', 'None', 'N/A'],
            keep_default_na=True,
            skipinitialspace=True
        )
        
        # Validate loaded data
        _validate_dataframe(df, file_path)
        
        logger.info(
            f"Successfully loaded {len(df)} employee records "
            f"with {len(df.columns)} columns from {file_path}"
        )
        
        return df
        
    except FileNotFoundError:
        logger.error(f"Employee data file not found: {file_path}")
        raise FileNotFoundError(f"Could not find employee data file: {file_path}")
    
    except pd.errors.EmptyDataError:
        logger.error(f"Employee data file is empty: {file_path}")  
        raise ValueError(f"Employee data file is empty: {file_path}")
    
    except pd.errors.ParserError as e:
        logger.error(f"Failed to parse employee data file {file_path}: {e}")
        raise pd.errors.ParserError(f"Malformed CSV file {file_path}: {e}")
    
    except PermissionError:
        logger.error(f"Permission denied accessing employee data file: {file_path}")
        raise PermissionError(f"Cannot read employee data file: {file_path}")
    
    except Exception as e:
        logger.error(f"Unexpected error loading employee data from {file_path}: {e}")
        raise RuntimeError(f"Failed to load employee data: {e}") from e

def _validate_dataframe(df: pd.DataFrame, file_path: Path) -> None:
    """
    Validate the loaded DataFrame meets basic requirements.
    
    Args:
        df: The loaded DataFrame
        file_path: Path to the source file for error messages
        
    Raises:
        ValueError: If DataFrame doesn't meet validation requirements
    """
    if df.empty:
        raise ValueError(f"Employee data file contains no data: {file_path}")
    
    if len(df.columns) == 0:
        raise ValueError(f"Employee data file contains no columns: {file_path}")
    
    return df


# Optional: Add a cached version for performance-critical applications
_cached_valid_emp_data: Optional[pd.DataFrame] = None

def valid_emp_cached(force_reload: bool = False) -> pd.DataFrame:
    """
    Load employee data with caching for improved performance.
    
    Args:
        force_reload: If True, bypass cache and reload from file
        
    Returns:
        pd.DataFrame: Employee data (potentially cached)
    """
    global _cached_valid_emp_data
    
    if _cached_valid_emp_data is None or force_reload:
        _cached_valid_emp_data = load_valid_emp_csv()
        logger.debug("Employee data loaded and cached")
    else:
        logger.debug("Using cached employee data")
    
    return _cached_valid_emp_data.copy()  # Return copy to prevent accidental modification