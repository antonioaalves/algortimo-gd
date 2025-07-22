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
from src.config import PROJECT_NAME, ROOT_DIR

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

# Set up logger
logger = get_logger(PROJECT_NAME)

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

    def __init__(self, parameters=None, algo_name: str = 'alcampo_algorithm', project_name: str = PROJECT_NAME, process_id: int = 0, start_date: str = '', end_date: str = ''):
        """
        Initialize the Alcampo Algorithm.
        
        Args:
            parameters: Dictionary containing algorithm configuration
            algo_name: Name identifier for the algorithm
        """
        # Default parameters for the algorithm
        default_parameters = {
            "shifts": ["M", "T", "L", "LQ", "F", "V", "LD", "A", "TC"],
            "check_shifts": ['M', 'T', 'L', 'LQ', "LD", "TC"],
            "check_shift_special": ['M', 'T', 'L', "TC"],
            "working_shifts": ["M", "T", "TC"],
            "max_continuous_working_days": 10,

            "settings":{
                #F days affect c2d and cxx
                "F_special_day": False,
                #defines if we should sum 2 day quality weekends with the number of free sundays
                "free_sundays_plus_c2d": False,
                "missing_days_afect_free_days": False,
            }
        }
        
        # Merge with provided parameters
        if parameters:
            default_parameters.update(parameters)
        
        # Initialize the parent class with algorithm name and parameters
        super().__init__(algo_name=algo_name, parameters=default_parameters, project_name=PROJECT_NAME)
        
        # Initialize algorithm-specific attributes
        self.data_processed = None
        self.model_stage1 = None
        self.model_stage2 = None
        self.schedule_stage1 = None
        self.final_schedule = None
        self.process_id = process_id
        self.start_date = start_date
        self.end_date = end_date
        
        # Add any algorithm-specific initialization
        self.logger.info(f"Initialized {self.algo_name} with parameters: {self.parameters}")



    def adapt_data(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Adapt input data for the shift scheduling algorithm.
        
        Args:
            data: Dictionary containing DataFrames:
                - Should contain medium_dataframes with 'matrizA_bk', 'matrizB_bk', 'matriz2_bk'
                
        Returns:
            Dictionary containing processed data elements for the algorithm
        """
        try:
            self.logger.info("Starting data adaptation for Alcampo algorithm")
            
            # =================================================================
            # 1. VALIDATE INPUT DATA STRUCTURE
            # =================================================================
            if data is None:
                raise ValueError("No data provided to adapt_data method. Expected dictionary with DataFrames.")
            
            if not isinstance(data, dict):
                raise TypeError(f"Expected dictionary, got {type(data)}")
            
            # Log the data structure for debugging
            self.logger.info(f"Input data structure: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            
            # Extract medium dataframes
            if 'medium_dataframes' in data:
                medium_dataframes = data['medium_dataframes']
                self.logger.info("Found nested medium_dataframes structure")
            else:
                medium_dataframes = data
                self.logger.info("Using direct DataFrame structure")
            
            if not isinstance(medium_dataframes, dict):
                raise TypeError(f"Expected medium_dataframes to be dictionary, got {type(medium_dataframes)}")
            
            # =================================================================
            # 2. VALIDATE REQUIRED DATAFRAMES
            # =================================================================
            # required_dataframes = ['matrizA_bk', 'matrizB_bk', 'matriz2_bk']
            # missing_dataframes = [df for df in required_dataframes if df not in medium_dataframes]
            
            # if missing_dataframes:
            #     self.logger.error(f"Missing required DataFrames: {missing_dataframes}")
            #     raise ValueError(f"Missing required DataFrames: {missing_dataframes}")
            
            # # Check if DataFrames are not empty
            # for df_name in required_dataframes:
            #     df = medium_dataframes[df_name]
            #     if df.empty:
            #         self.logger.error(f"DataFrame {df_name} is empty")
            #         raise ValueError(f"DataFrame {df_name} is empty")
                
            #     self.logger.info(f"✅ {df_name}: {df.shape} - {df.memory_usage(deep=True).sum()/1024/1024:.2f} MB")
            
            # =================================================================
            # 3. PROCESS DATA USING ENHANCED FUNCTION
            # =================================================================
            self.logger.info("Calling enhanced data processing function")
            
            # Import the enhanced function
            from src.algorithms.model_alcampo.read_alcampos import read_data_alcampo
            
            processed_data = read_data_alcampo(medium_dataframes)
            
            # =================================================================
            # 4. UNPACK AND VALIDATE PROCESSED DATA
            # =================================================================
            self.logger.info("Unpacking processed data")
            
            try:
                data_dict = {
                    'matriz_calendario_gd': processed_data[0],
                    'days_of_year': processed_data[1],
                    'sundays': processed_data[2],
                    'holidays': processed_data[3],
                    'special_days': processed_data[4],
                    'closed_holidays': processed_data[5],
                    'empty_days': processed_data[6],
                    'worker_holiday': processed_data[7],
                    'missing_days': processed_data[8],
                    'working_days': processed_data[9],
                    'non_holidays': processed_data[10],
                    'start_weekday': processed_data[11],
                    'week_to_days': processed_data[12],
                    'worker_week_shift': processed_data[13],
                    'matriz_colaboradores_gd': processed_data[14],
                    'workers': processed_data[15],
                    'contract_type': processed_data[16],
                    'total_l': processed_data[17],
                    'total_l_dom': processed_data[18],
                    'c2d': processed_data[19],
                    'c3d': processed_data[20],
                    'l_d': processed_data[21],
                    'l_q': processed_data[22],
                    'cxx': processed_data[23],
                    't_lq': processed_data[24],
                    'tc': processed_data[25],
                    'matriz_estimativas_gd': processed_data[26],
                    'pessObj': processed_data[27],
                    'min_workers': processed_data[28],
                    'max_workers': processed_data[29],
                    'working_shift_2': processed_data[30],
                    'workers_complete': processed_data[31],
                    'workers_complete_cycle': processed_data[32],
                    'free_day_complete_cycle': processed_data[33]
                }

            except IndexError as e:
                self.logger.error(f"Error unpacking processed data: {e}")
                raise ValueError(f"Invalid data structure returned from processing function: {e}")
            
            # =================================================================
            # 5. FINAL VALIDATION AND LOGGING
            # =================================================================
            workers = data_dict['workers']
            workers_complete = data_dict['workers_complete']
            days_of_year = data_dict['days_of_year']
            special_days = data_dict['special_days']
            working_days = data_dict['working_days']

            for w in workers_complete:
                self.logger.info(f"Worker {w}, working days: {working_days[w]}, special days: {special_days}")
            
            for w in workers:
                self.logger.info(f"Worker {w}, working days: {working_days[w]}, special days: {special_days}")

            # Validate critical data
            if not workers_complete:
                raise ValueError("No valid workers found after processing")
            
            if not days_of_year:
                raise ValueError("No valid days found after processing")
            
            # Log final statistics
            self.logger.info("[OK] Data adaptation completed successfully")
            self.logger.info(f"[STATS] Final statistics:")
            self.logger.info(f"   Total workers: {len(workers_complete)}")
            self.logger.info(f"   Valid workers (cycle_not complete): {len(workers)}")
            self.logger.info(f"   Total days: {len(days_of_year)}")
            self.logger.info(f"   Working days: {len(working_days)}")
            self.logger.info(f"   Special days: {len(special_days)}")
            self.logger.info(f"   Week mappings: {len(data_dict['week_to_days'])}")
            
            # Store processed data in instance
            self.data_processed = data_dict
            
            return data_dict
            
        except Exception as e:
            self.logger.error(f"Error in data adaptation: {e}", exc_info=True)
            raise


    def execute_algorithm(self, adapted_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Execute the two-stage Alcampo shift scheduling algorithm.
        
        Args:
            adapted_data: Processed data from adapt_data method
            
        Returns:
            Final schedule DataFrame
        """
        try:
            self.logger.info("Starting Alcampo algorithm execution")
            
            if adapted_data is None:
                adapted_data = self.data_processed
            
            # Extract data elements
            matriz_calendario_gd = adapted_data['matriz_calendario_gd']
            days_of_year = adapted_data['days_of_year']
            sundays = adapted_data['sundays']
            holidays = adapted_data['holidays']
            special_days = adapted_data['special_days']
            closed_holidays = adapted_data['closed_holidays']
            empty_days = adapted_data['empty_days']
            worker_holiday = adapted_data['worker_holiday']
            missing_days = adapted_data['missing_days']
            working_days = adapted_data['working_days']
            non_holidays = adapted_data['non_holidays']
            start_weekday = adapted_data['start_weekday']
            week_to_days = adapted_data['week_to_days']
            worker_week_shift = adapted_data['worker_week_shift']
            workers = adapted_data['workers']
            contract_type = adapted_data['contract_type']
            total_l = adapted_data['total_l']
            total_l_dom = adapted_data['total_l_dom']
            c2d = adapted_data['c2d']
            c3d = adapted_data['c3d']
            l_d = adapted_data['l_d']
            l_q = adapted_data['l_q']
            cxx = adapted_data['cxx']
            t_lq = adapted_data['t_lq']
            tc = adapted_data['tc']
            pessObj = adapted_data['pessObj']
            min_workers = adapted_data['min_workers']
            max_workers = adapted_data['max_workers']
            working_shift_2 = adapted_data['working_shift_2']
            workers_complete = adapted_data['workers_complete']
            workers_complete_cycle = adapted_data['workers_complete_cycle']
            free_day_complete_cycle = adapted_data['free_day_complete_cycle']

            # Extract algorithm parameters
            shifts = self.parameters["shifts"]
            check_shift = self.parameters["check_shifts"]
            check_shift_special = self.parameters["check_shift_special"]
            working_shift = self.parameters["working_shifts"]
            max_continuous_days = self.parameters["max_continuous_working_days"]
            
            # =================================================================
            # STAGE 1: Initial scheduling with all constraints
            # =================================================================
            self.logger.info("Starting Stage 1: Initial scheduling")
            
            model = cp_model.CpModel()
            self.model_stage1 = model
            
            self.logger.info("Model initialized for Stage 1")

            # Create decision variables
            shift = decision_variables(model, days_of_year, workers_complete, shifts)

            self.logger.info("Decision variables created for Stage 1")
            
            # Apply all constraints
            self._apply_stage1_constraints(
                                 model, shift, days_of_year, workers, shifts, check_shift, 
                                 check_shift_special, working_shift, max_continuous_days, week_to_days,
                                 working_shift_2, contract_type, special_days, total_l, c2d, c3d, working_days,
                                 total_l_dom, tc, l_d, l_q, cxx, closed_holidays, worker_holiday,
                                 missing_days, empty_days, worker_week_shift, start_weekday, sundays,
                                 t_lq, matriz_calendario_gd, workers_complete, workers_complete_cycle,
                                 free_day_complete_cycle
)

            self.logger.info("Constraints applied for Stage 1")
            
            # Set up optimization objective
            optimization_prediction(
                model, days_of_year, workers_complete, workers_complete_cycle, working_shift, shift, pessObj, 
                min_workers, closed_holidays, week_to_days, working_days, contract_type
            )
            
            # Solve Stage 1
            self.logger.info("Solving Stage 1 model")
            schedule_df = solve(model, days_of_year, workers_complete, special_days, shift, shifts, max_time_seconds=60, output_filename=os.path.join(ROOT_DIR, 'data', 'output', f'working_schedule_{self.process_id}-stage1.xlsx'))
            self.schedule_stage1 = pd.DataFrame(schedule_df).copy()
            
            # =================================================================
            # STAGE 2: Refinement with 3-day weekend constraints
            # =================================================================
            self.logger.info("Starting Stage 2: Schedule refinement")
            
            new_model = cp_model.CpModel()
            self.model_stage2 = new_model
            
            # Create new decision variables
            new_shift = decision_variables(new_model, days_of_year, workers_complete, shifts)
            
            # Apply Stage 2 constraints
            self._apply_stage2_constraints(new_model, new_shift, days_of_year, workers, shifts,
                                 total_l, working_days, l_q, c2d, c3d, schedule_df, start_weekday,
                                 contract_type, closed_holidays, workers_complete, workers_complete_cycle)
            
            # Apply optimization (reusing from Stage 1)
            optimization_prediction(
                new_model, days_of_year, workers_complete, workers_complete_cycle,working_shift, new_shift, pessObj,
                min_workers, closed_holidays, week_to_days, working_days, contract_type
            )
            
            
            # Solve Stage 2
            self.logger.info("Solving Stage 2 model")
            final_schedule_df = solve(new_model, days_of_year, workers_complete, special_days, new_shift, shifts, max_time_seconds=60, output_filename=os.path.join(ROOT_DIR, 'data', 'output', f'working_schedule_{self.process_id}-stage2.xlsx'))
            #final_schedule_df = solve_alcampo(adapted_data, shifts, check_shift, check_shift_special, working_shift, max_continuous_days)
            self.final_schedule = pd.DataFrame(final_schedule_df).copy()
            
            self.logger.info("Alcampo algorithm execution completed successfully")
            return final_schedule_df
            
        except Exception as e:
            self.logger.error(f"Error in algorithm execution: {e}", exc_info=True)
            raise

    def _apply_stage1_constraints(self, model, shift, days_of_year, workers, shifts, check_shift, 
                                 check_shift_special, working_shift, max_continuous_days, week_to_days,
                                 working_shift_2, contract_type, special_days, total_l, c2d, c3d, working_days,
                                 total_l_dom, tc, l_d, l_q, cxx, closed_holidays, worker_holiday,
                                 missing_days, empty_days, worker_week_shift, start_weekday, sundays,
                                 t_lq, matriz_calendario_gd, workers_complete, workers_complete_cycle,
                                 free_day_complete_cycle):
        """Apply all Stage 1 constraints to the model."""
        

        shift_day_constraint(model, shift, days_of_year, workers_complete, shifts)
        
        # Constraint to limit working days in a week based on contract type
        week_working_days_constraint(model, shift, week_to_days, workers, working_shift_2, contract_type)
        
        # Constraint to limit maximum continuous working days
        maximum_continuous_working_days(model, shift, days_of_year, workers, working_shift, max_continuous_days)
        
        # Constraint to limit maximum continuous working special days
        maximum_continuous_working_special_days(model, shift, special_days, workers, working_shift, contract_type)
        
        # Constraint to limit maximum free days in a year
        maximum_free_days(model, shift, days_of_year, workers, total_l, c3d)
        
        # Constraint for free days on special days
        # free_days_special_days(model, shift, special_days, workers, working_days, total_l_dom)
        
        # TC attribution constraint
        # tc_atribution(model, shift, workers, days_of_year, tc, special_days, working_days)
        
        # Working days special days constraint
        working_days_special_days(model, shift, special_days, workers, working_days, l_d, contract_type)
        
        # LQ attribution constraint
        LQ_attribution(model, shift, workers, working_days, l_q, c2d)
        
        # LD attribution constraint
        LD_attribution(model, shift, workers, working_days, l_d)
        
        # Closed holiday attribution
        closed_holiday_attribution(model, shift, workers_complete, closed_holidays)
        
        # Holiday, missing days and empty days attribution
        holiday_missing_day_attribution(model, shift, workers_complete, worker_holiday, missing_days, empty_days, free_day_complete_cycle)
        
        # Worker week shift assignments #####
        assign_week_shift(model, shift, workers_complete, week_to_days, working_days, worker_week_shift)
        
        # Working day shifts constraint
        working_day_shifts(model, shift, workers, working_days, check_shift)
        
        # Special day shifts constraint
        special_day_shifts(model, shift, workers, special_days, check_shift_special, working_days)

        # Complete cycle shifts constraint
        complete_cycle_shifts(model, shift, workers_complete_cycle, working_days, working_shift_2)
        
        # Free days adjacent to weekends
        free_day_next_2c(model, shift, workers, working_days, start_weekday, closed_holidays)
        
        # Limit consecutive free days during the week
        no_free__days_close(model, shift, workers, working_days, start_weekday, week_to_days, cxx, contract_type, closed_holidays, days_of_year)
        
        # Day2 quality weekends
        day2_quality_weekend(model, shift, workers, working_days, sundays, c2d, contract_type, closed_holidays)
        
        # Space LQs constraint
        space_LQs(model, shift, workers, working_days, t_lq, matriz_calendario_gd)
        
        # Priority 2-3 workers constraint
        prio_2_3_workers(model, shift, workers, working_days, special_days, start_weekday, 
                        week_to_days, contract_type, working_shift)
        
        # Compensation days constraint
        compensation_days(model, shift, workers, working_days, special_days, start_weekday, 
                     week_to_days, contract_type, working_shift)
        
        # Limits LDs per week
        limits_LDs_week(model, shift, week_to_days, workers, special_days)
    
        # One free day weekly
        one_free_day_weekly(model, shift, week_to_days, workers, working_days, contract_type, closed_holidays)

    def _apply_stage2_constraints(self, new_model, new_shift, days_of_year, workers, shifts,
                                 total_l, working_days, l_q, c2d, c3d, schedule_df, start_weekday,
                                 contract_type, closed_holidays, workers_complete, workers_complete_cycle):
        """Apply Stage 2 specific constraints."""
        
        # Constraint for workers having an assigned shift for each day
        shift_day_constraint(new_model, new_shift, days_of_year, workers, shifts)
        
        # Constraint for maximum free days in a year
        maxi_free_days_c3d(new_model, new_shift, workers, days_of_year, total_l)
        
        # Constraint for maximum LQ days in a year
        maxi_LQ_days_c3d(new_model, new_shift, workers, working_days, l_q, c2d, c3d)
        
        # Assign solution days based on the previous schedule
        assigns_solution_days(new_model, new_shift, workers_complete, workers_complete_cycle, days_of_year, schedule_df, working_days, start_weekday, shifts)
        
        # Constraint for 3-day quality weekends
        day3_quality_weekend(new_model, new_shift, workers, working_days, start_weekday, 
                            schedule_df, c3d, contract_type, closed_holidays)

    def format_results(self, algorithm_results: pd.DataFrame = pd.DataFrame()) -> Dict[str, Any]:
        """
        Format the algorithm results for output.
        
        Args:
            algorithm_results: Final schedule DataFrame from execute_algorithm
            
        Returns:
            Dictionary containing formatted results and metadata
        """
        try:
            self.logger.info("Formatting Alcampo algorithm results")

            # TODO: check what is the default logic
            #if algorithm_results is None:
            #    algorithm_results = self.final_schedule
            #
            #if algorithm_results is None:
            #    raise ValueError("No algorithm results available to format")
            #
            ## My code:
            #if algorithm_results.empty:
            #    self.logger.warning("No algorithm results available to format")
            #    algorithm_results = self.final_schedule if self.final_schedule is not None else pd.DataFrame()

            # Get result dfs
            stage1_schedule = pd.DataFrame(self.schedule_stage1).copy()
            stage2_schedule = pd.DataFrame(self.final_schedule).copy()

            # Calculate some basic statistics
            total_workers = len(algorithm_results['Worker'].unique()) if 'Worker' in algorithm_results.columns else 0
            total_days = len(algorithm_results['Day'].unique()) if 'Day' in algorithm_results.columns else 0
            total_assignments = len(algorithm_results)
            
            # Count shift distributions
            shift_distribution = {}
            if 'Shift' in algorithm_results.columns:
                shift_distribution = algorithm_results['Shift'].value_counts().to_dict()
            
            # DEBUG: R logic convertion 
            # Equivalent to the R naming operation
            date_range = pd.date_range(start=self.start_date, end=self.end_date, freq='D')
            new_columns = ['Worker'] + [str(date) for date in date_range]
            stage1_schedule.columns = new_columns

            # Equivalent to reshape2::melt
            stage1_schedule = pd.melt(stage1_schedule, 
                                    id_vars=['Worker'], 
                                    var_name='Date', 
                                    value_name='Status')

            date_range = pd.date_range(start=self.start_date, end=self.end_date, freq='D')
            new_columns = ['Worker'] + [str(date) for date in date_range]
            stage2_schedule.columns = new_columns

            # Equivalent to reshape2::melt
            stage2_schedule = pd.melt(stage2_schedule, 
                                    id_vars=['Worker'], 
                                    var_name='Date', 
                                    value_name='Status')            

            formatted_results = {
                'schedule': algorithm_results,
                'metadata': {
                    'algorithm_name': self.algo_name,
                    'total_workers': total_workers,
                    'total_days': total_days,
                    'total_assignments': total_assignments,
                    'shift_distribution': shift_distribution,
                    'execution_timestamp': datetime.now().isoformat(),
                    'parameters_used': self.parameters
                },
                'stage1_schedule': self.schedule_stage1,
                'stage2_schedule': self.final_schedule,
                'summary': {
                    'status': 'completed',
                    'message': f'Successfully scheduled {total_workers} workers over {total_days} days'
                }
            }


            
            self.logger.info(f"Results formatted successfully: {total_assignments} assignments created")
            return formatted_results
            
        except Exception as e:
            self.logger.error(f"Error in results formatting: {e}", exc_info=True)
            raise

    def run(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Run the complete algorithm pipeline: adapt_data -> execute_algorithm -> format_results.
        
        Args:
            data: Input data dictionary containing DataFrames
            
        Returns:
            Formatted results dictionary
        """
        self.logger.info("Running full Alcampo algorithm pipeline")
        
        # Step 1: Adapt data
        adapted_data = self.adapt_data(data)
        
        # Step 2: Execute algorithm
        results = self.execute_algorithm(adapted_data)
        
        # Step 3: Format results
        formatted_results = self.format_results(results)
        
        self.logger.info("Full algorithm pipeline completed successfully")

        return formatted_results