"""File containing the AlcampoAlgorithm class"""

import logging
from typing import Dict, Any, Optional, List, Union
import pandas as pd
from datetime import datetime, timedelta
from ortools.sat.python import cp_model
import os

# Import base algorithm class
from base_data_project.algorithms.base import BaseAlgorithm
from base_data_project.log_config import get_logger

# Import project-specific components
from src.configuration_manager import ConfigurationManager

# Global configuration manager instance (singleton pattern)
_config_manager = None

def get_config_manager() -> ConfigurationManager:
    """
    Get or create the global configuration manager instance.
    
    This function implements the singleton pattern - creates the config manager
    once and reuses it across all calls, ensuring consistency and performance.
    
    Returns:
        ConfigurationManager: Shared configuration manager instance
    """
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigurationManager()
    return _config_manager

# Import shift scheduler components
from src.algorithms.model_alcampo.variables import decision_variables
from src.algorithms.model_alcampo.alcampo_constraints import (
    shift_day_constraint, week_working_days_constraint, maximum_continuous_working_days,
    maximum_continuous_working_special_days, maximum_free_days, free_days_special_days, 
    tc_atribution, working_days_special_days, LQ_attribution, LD_attribution, 
    closed_holiday_attribution, holiday_missing_day_attribution, assign_week_shift,
    special_day_shifts, working_day_shifts, complete_cycle_shifts, free_day_next_2c, no_free__days_close, 
    space_LQs, day2_quality_weekend, compensation_days, prio_2_3_workers,
    limits_LDs_week, one_free_day_weekly, maxi_free_days_c3d, maxi_LQ_days_c3d, 
    assigns_solution_days, day3_quality_weekend
)
from src.algorithms.model_alcampo.optimization_alcampos import optimization_prediction
from src.algorithms.solver.solver import solve

# Initialize logger with project name from config
logger = get_logger(get_config_manager().project_name)

class AlcampoAlgorithm(BaseAlgorithm):
    """
    Alcampo shift scheduling algorithm implementation.

    This algorithm implements a two-stage constraint programming approach for shift scheduling:
    1. Adapt data: Read and process input DataFrames (calendario, estimativas, colaborador)
    2. Execute algorithm: 
       - Stage 1: Solve initial scheduling problem with all constraints
       - Stage 2: Refine solution with additional quality constraints for 3-day weekends
    3. Format results: Return final schedule DataFrame

    The algorithm uses OR-Tools CP-SAT solver to optimize shift assignments while respecting
    worker contracts, labor laws, and operational requirements.
    """

    def __init__(self, parameters=None, algo_name: str = 'alcampo_algorithm', 
                 project_name: str = None, process_id: int = 0, 
                 start_date: str = '', end_date: str = '', 
                 config_manager: ConfigurationManager = None):
        """
        Initialize the Alcampo Algorithm.
        
        Args:
            parameters: Dictionary containing algorithm configuration
            algo_name: Name identifier for the algorithm
            project_name: Project name (will use config if not provided)
            process_id: Process ID for tracking
            start_date: Start date for processing
            end_date: End date for processing
            config_manager: Configuration manager instance (will use singleton if not provided)
        """
        # Use provided config manager or singleton
        self.config = config_manager if config_manager else get_config_manager()
        
        # Use project name from config if not provided
        if project_name is None:
            project_name = self.config.project_name
            
        self.logger = get_logger(project_name)
        self.logger.info(f"Initializing {algo_name} algorithm")
        
        # Validate algorithm is available
        if not self.config.system.has_algorithm(algo_name):
            available = self.config.system.get_algorithm_list()
            error_msg = f"Algorithm '{algo_name}' not available. Available algorithms: {available}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Store algorithm configuration
        self.algo_name = algo_name
        self.project_name = project_name
        self.process_id = process_id
        self.start_date = start_date
        self.end_date = end_date
        
        # Merge parameters with defaults
        if parameters is None:
            self.parameters = self.config.parameters.get_algorithm_config(algo_name)
        else:
            # Merge with algorithm-specific defaults
            algo_defaults = self.config.parameters.get_algorithm_config(algo_name)
            self.parameters = {**algo_defaults, **parameters}
        
        # Initialize parent class
        super().__init__(
            algo_name=algo_name,
            parameters=self.parameters
        )
        
        self.logger.info(f"Algorithm {algo_name} initialized successfully")
        self.logger.debug(f"Parameters: {self.parameters}")

    def adapt_data(self, data=None):
        """
        Adapt data for the Alcampo algorithm.
        
        Args:
            data: Input data (DataFrames dictionary or similar)
            
        Returns:
            Adapted data ready for algorithm processing
        """
        self.logger.info("Starting data adaptation for Alcampo algorithm")
        
        try:
            # Import the read function here to avoid circular imports
            from src.algorithms.model_alcampo.read_alcampos import read_data_alcampo
            
            # Read and adapt the data using the specific function
            adapted_data = read_data_alcampo(data)
            
            self.logger.info("Data adaptation completed successfully")
            return adapted_data
            
        except Exception as e:
            error_msg = f"Error during data adaptation: {str(e)}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

    def execute_algorithm(self, adapted_data=None):
        """
        Execute the Alcampo scheduling algorithm.
        
        Args:
            adapted_data: Data prepared by adapt_data method
            
        Returns:
            Algorithm results
        """
        self.logger.info("Starting Alcampo algorithm execution")
        
        try:
            # Extract adapted data components
            (matriz_calendario_gd, matriz_estimativas_gd, matriz_colaborador_gd,
             colab_emp, colab_cod, colab_contrato, estimativas_values, num_days,
             num_employees, emp_range, days_range, start_date, days_off_by_employee,
             absent_days_by_employee, holidays_by_employee, calendario_dataframe,
             shifts_mapping, shifts_reverse_mapping, shifts_names, max_estimativas_value,
             max_colab_value, max_days_value) = adapted_data
            
            self.logger.info("Creating decision variables")
            # Create decision variables
            model = cp_model.CpModel()
            x = decision_variables(model, num_employees, num_days, len(shifts_names))
            
            self.logger.info("Adding constraints to the model")
            # Add all constraints
            
            # Basic constraints
            shift_day_constraint(model, x, num_employees, num_days, len(shifts_names))
            week_working_days_constraint(model, x, num_employees, num_days, len(shifts_names), colab_contrato)
            maximum_continuous_working_days(model, x, num_employees, num_days, len(shifts_names), colab_contrato)
            maximum_continuous_working_special_days(model, x, num_employees, num_days, len(shifts_names), 
                                                   calendario_dataframe, start_date)
            maximum_free_days(model, x, num_employees, num_days, len(shifts_names), colab_contrato)
            free_days_special_days(model, x, num_employees, num_days, len(shifts_names), 
                                 calendario_dataframe, start_date)
            
            # Shift assignment constraints
            tc_atribution(model, x, num_employees, num_days, len(shifts_names), shifts_names, 
                         colab_contrato, calendario_dataframe, start_date)
            working_days_special_days(model, x, num_employees, num_days, len(shifts_names), 
                                    calendario_dataframe, start_date, colab_contrato)
            LQ_attribution(model, x, num_employees, num_days, len(shifts_names), shifts_names, 
                          colab_contrato)
            LD_attribution(model, x, num_employees, num_days, len(shifts_names), shifts_names, 
                          colab_contrato)
            
            # Holiday and absence constraints
            closed_holiday_attribution(model, x, num_employees, num_days, len(shifts_names), 
                                      calendario_dataframe, start_date)
            holiday_missing_day_attribution(model, x, num_employees, num_days, len(shifts_names), 
                                          holidays_by_employee, absent_days_by_employee, 
                                          days_off_by_employee, start_date)
            
            # Weekly and daily constraints
            assign_week_shift(model, x, num_employees, num_days, len(shifts_names), shifts_names, 
                            colab_contrato, calendario_dataframe, start_date)
            special_day_shifts(model, x, num_employees, num_days, len(shifts_names), 
                             calendario_dataframe, start_date, shifts_names)
            working_day_shifts(model, x, num_employees, num_days, len(shifts_names), 
                             calendario_dataframe, start_date, shifts_names)
            complete_cycle_shifts(model, x, num_employees, num_days, len(shifts_names), 
                                shifts_names, colab_contrato)
            
            # Quality constraints
            free_day_next_2c(model, x, num_employees, num_days, len(shifts_names), shifts_names)
            no_free__days_close(model, x, num_employees, num_days, len(shifts_names), shifts_names)
            space_LQs(model, x, num_employees, num_days, len(shifts_names), shifts_names)
            day2_quality_weekend(model, x, num_employees, num_days, len(shifts_names), 
                                calendario_dataframe, start_date, shifts_names)
            compensation_days(model, x, num_employees, num_days, len(shifts_names), 
                            calendario_dataframe, start_date, shifts_names, colab_contrato)
            prio_2_3_workers(model, x, num_employees, num_days, len(shifts_names), 
                           calendario_dataframe, start_date, shifts_names, colab_contrato)
            limits_LDs_week(model, x, num_employees, num_days, len(shifts_names), 
                          calendario_dataframe, start_date, shifts_names, colab_contrato)
            one_free_day_weekly(model, x, num_employees, num_days, len(shifts_names), 
                               calendario_dataframe, start_date, colab_contrato)
            maxi_free_days_c3d(model, x, num_employees, num_days, len(shifts_names), 
                              calendario_dataframe, start_date, colab_contrato)
            maxi_LQ_days_c3d(model, x, num_employees, num_days, len(shifts_names), 
                            calendario_dataframe, start_date, shifts_names, colab_contrato)
            
            self.logger.info("Setting optimization objective")
            # Set optimization objective
            optimization_prediction(model, x, num_employees, num_days, len(shifts_names), 
                                   estimativas_values, shifts_names)
            
            self.logger.info("Solving stage 1 of the algorithm")
            # Solve the model (Stage 1)
            solution_stage1 = solve(model, x, num_employees, num_days, len(shifts_names), 
                                  colab_cod, start_date, shifts_names)
            
            if solution_stage1 is None:
                self.logger.warning("Stage 1 solution not found, returning empty results")
                return None
            
            self.logger.info("Stage 1 completed successfully, starting stage 2")
            
            # Stage 2: Add additional quality constraints
            assigns_solution_days(model, x, num_employees, num_days, len(shifts_names), 
                                solution_stage1, shifts_names)
            day3_quality_weekend(model, x, num_employees, num_days, len(shifts_names), 
                               calendario_dataframe, start_date, shifts_names)
            
            self.logger.info("Solving stage 2 of the algorithm")
            # Solve the enhanced model (Stage 2)
            final_solution = solve(model, x, num_employees, num_days, len(shifts_names), 
                                 colab_cod, start_date, shifts_names)
            
            if final_solution is None:
                self.logger.warning("Stage 2 solution not found, using stage 1 results")
                final_solution = solution_stage1
            
            self.logger.info("Alcampo algorithm execution completed successfully")
            return final_solution
            
        except Exception as e:
            error_msg = f"Error during algorithm execution: {str(e)}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

    def format_results(self, algorithm_results=None):
        """
        Format the algorithm results for output.
        
        Args:
            algorithm_results: Raw results from execute_algorithm
            
        Returns:
            Formatted results (DataFrame or dict)
        """
        self.logger.info("Formatting Alcampo algorithm results")
        
        try:
            if algorithm_results is None:
                self.logger.warning("No algorithm results to format")
                return None
            
            # The solve function already returns a formatted DataFrame
            # Additional formatting can be added here if needed
            formatted_results = algorithm_results
            
            self.logger.info(f"Results formatted successfully. Shape: {formatted_results.shape if hasattr(formatted_results, 'shape') else 'N/A'}")
            return formatted_results
            
        except Exception as e:
            error_msg = f"Error during result formatting: {str(e)}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)