"""File containing the SalsaEspAlgorithm class"""

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
from src.configuration_manager.instance import get_config

# Import shift scheduler components
from src.algorithms.model_salsa_esp.variables import decision_variables
from src.algorithms.model_salsa_esp.salsa_esp_constraints import (
    free_days_special_days, shift_day_constraint, week_working_days_constraint, maximum_continuous_working_days,
    LQ_attribution, compensation_days, assign_week_shift, working_day_shifts, salsa_esp_2_consecutive_free_days,
    salsa_esp_2_day_quality_weekend, salsa_esp_saturday_L_constraint, salsa_esp_2_free_days_week, first_day_not_free,
    free_days_special_days
)
from src.algorithms.model_salsa_esp.optimization_salsa_esp import salsa_esp_optimization
from src.algorithms.solver.solver import solve

from src.helpers import (_create_empty_results, _calculate_comprehensive_stats, 
                        _validate_constraints, _calculate_quality_metrics, 
                        _format_schedules, _create_metadata, _validate_solution, 
                        _create_export_info)


# Create configuration manager and set up logger
_config_manager = get_config()
logger = get_logger(_config_manager.project_name)

class SalsaEspAlgorithm(BaseAlgorithm):
    """
    SALSA shift scheduling algorithm implementation.

    This algorithm implements a constraint programming approach for shift scheduling:
    1. Adapt data: Read and process input DataFrames (calendario, estimativas, colaborador)
    2. Execute algorithm: Solve scheduling problem with SALSA-specific constraints
    3. Format results: Return final schedule DataFrame

    The algorithm uses OR-Tools CP-SAT solver to optimize shift assignments while respecting
    worker contracts, labor laws, and SALSA-specific operational requirements.
    """

    def __init__(self, parameters=None, algo_name: str = 'salsa_esp_algorithm', project_name: str = None, process_id: int = 0, start_date: str = '', end_date: str = ''):
        """
        Initialize the SALSA Algorithm.
        
        Args:
            parameters: Dictionary containing algorithm configuration
            algo_name: Name identifier for the algorithm
            project_name: Project name
            process_id: Process identifier
            start_date: Start date for scheduling
            end_date: End date for scheduling
        """
        # Set default project name if not provided
        if project_name is None:
            project_name = _config_manager.project_name
            
        # Default parameters for the SALSA algorithm
        default_parameters = {
            "shifts": ['M', 'T', 'L', 'LQ', 'LD', 'F', 'A', 'V', '-'],
            "check_shifts": ['M', 'T', 'L', 'LQ', 'LD'],
            "working_shifts": ['M', 'T', 'LD'],
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
        super().__init__(algo_name=algo_name, parameters=default_parameters, project_name=project_name)
        
        # Initialize algorithm-specific attributes
        self.data_processed = None
        self.model = None
        self.final_schedule = None
        self.process_id = process_id
        self.start_date = start_date
        self.end_date = end_date
        
        # Add any algorithm-specific initialization
        self.logger.info(f"Initialized {self.algo_name} with parameters: {self.parameters}")

    def adapt_data(self, data: Dict[str, pd.DataFrame], algorithm_treatment_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Adapt input data for the SALSA shift scheduling algorithm.
        
        Args:
            data: Dictionary containing DataFrames:
                - Should contain medium_dataframes with calendar, estimates, and collaborator data
            algorithm_treatment_params: Dictionary containing algorithm treatment parameters
                
        Returns:
            Dictionary containing processed data elements for the algorithm
        """
        try:
            self.logger.info("Starting data adaptation for SALSA algorithm")
            
            # Handle treatment parameters - use empty dict if None
            if algorithm_treatment_params is None:
                algorithm_treatment_params = {}
                self.logger.debug("No algorithm treatment parameters provided, using empty dict")
            else:
                self.logger.info(f"Using algorithm treatment parameters: {list(algorithm_treatment_params.keys())}")
            
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
            # 3. PROCESS DATA USING SALSA FUNCTION
            # =================================================================
            self.logger.info("Calling SALSA data processing function")
            
            # Import the SALSA data processing function
            from src.algorithms.model_salsa_esp.read_salsa_esp import read_data_salsa_esp
            
            processed_data = read_data_salsa_esp(medium_dataframes, algorithm_treatment_params)
            
            
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
                    'matriz_colaborador_gd': processed_data[14],
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
                    'matriz_estimativas_gd': processed_data[25],
                    'pess_obj': processed_data[26],
                    'min_workers': processed_data[27],
                    'max_workers': processed_data[28],
                    'working_shift_2': processed_data[29],  # Adjusted for SALSA
                    'workers_complete': processed_data[30],  # Adjusted for SALSA
                    'workers_complete_cycle': processed_data[31],  # Adjusted for SALSA
                    'free_day_complete_cycle': processed_data[32],  # Adjusted for SALSA
                    'week_to_days_salsa_esp': processed_data[33],  # Adjusted for SALSA
                    'first_registered_day': processed_data[34],
                    'admissao_proporcional': processed_data[35],
                    'role_by_worker': processed_data[36],  # New role mapping
                    #'managers': processed_data[37],  # New managers list
                    #'keyholders': processed_data[38],  # New keyholders list
                    'data_admissao': processed_data[37],
                    'data_demissao': processed_data[38],
                    'last_registered_day': processed_data[39],
                    'fixed_days_off': processed_data[40],
                    'proportion': processed_data[41],
                    'fixed_LQs' : processed_data[42],
                    # 'week_cut': processed_data[34]
                    'work_day_hours': processed_data[43],
                    'work_days_per_week': processed_data[44],
                    'week_compensation_limit': processed_data[45],
                    'num_dias_cons': processed_data[46] 
                }

            except IndexError as e:
                self.logger.error(f"Error unpacking processed data: {e}")
                raise ValueError(f"Invalid data structure returned from processing function: {e}")
            
            # =================================================================
            # 5. FINAL VALIDATION AND LOGGING
            # =================================================================
            workers = data_dict['workers']
            days_of_year = data_dict['days_of_year']
            special_days = data_dict['special_days']
            working_days = data_dict['working_days']

            # Validate critical data
            if not workers:
                raise ValueError("No valid workers found after processing")
            
            if not days_of_year:
                raise ValueError("No valid days found after processing")
            
            # Log final statistics
            self.logger.info("[OK] Data adaptation completed successfully")
            self.logger.info(f"[STATS] Final statistics:")
            self.logger.info(f"   Total workers: {len(workers)}")
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
        Execute the SALSA shift scheduling algorithm.
        
        Args:
            adapted_data: Processed data from adapt_data method
            
        Returns:
            Final schedule DataFrame
        """
        try:
            self.logger.info("Starting SALSA algorithm execution")
            
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
            matriz_colaborador_gd = adapted_data['matriz_colaborador_gd']
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
            matriz_estimativas_gd = adapted_data['matriz_estimativas_gd']
            pessObj = adapted_data['pess_obj']
            min_workers = adapted_data['min_workers']
            max_workers = adapted_data['max_workers']
            workers_complete = adapted_data['workers_complete']
            workers_complete_cycle = adapted_data['workers_complete_cycle']
            free_day_complete_cycle = adapted_data['free_day_complete_cycle']
            week_to_days_salsa_esp = adapted_data['week_to_days_salsa_esp']
            first_day = adapted_data['first_registered_day']
            admissao_proporcional = adapted_data['admissao_proporcional']
            data_admissao = adapted_data['data_admissao']
            data_demissao = adapted_data['data_demissao']
            last_day = adapted_data['last_registered_day']
            fixed_days_off = adapted_data['fixed_days_off']
            fixed_LQs = adapted_data['fixed_LQs']
            role_by_worker = adapted_data['role_by_worker']
            #managers = adapted_data['managers']
            #keyholders = adapted_data['keyholders']
            # week_cut = adapted_data['week_cut']
            proportion = adapted_data['proportion']
            work_day_hours = adapted_data['work_day_hours']
            work_days_per_week = adapted_data['work_days_per_week']
            week_compensation_limit = adapted_data['week_compensation_limit']
            max_continuous_days = adapted_data["num_dias_cons"]

            # Extract algorithm parameters
            shifts = self.parameters["shifts"]
            check_shift = self.parameters["check_shifts"]
            working_shift = self.parameters["working_shifts"]
            
            # Extract settings
            settings = self.parameters["settings"]
            F_special_day = settings["F_special_day"]
            free_sundays_plus_c2d = settings["free_sundays_plus_c2d"]
            missing_days_afect_free_days = settings["missing_days_afect_free_days"]

            #   # === TEST: remover totalmente um worker problemático ===
            # DROP_W = 80001744
            # logger.warning(f"[TEST] Dropping worker {DROP_W} for feasibility test")

            # # 1) listas de workers
            # workers = [w for w in workers if w != DROP_W]
            # workers_complete = [w for w in workers_complete if w != DROP_W]
            # workers_complete_cycle = [w for w in workers_complete_cycle if w != DROP_W]

            # # 2) dicionários por worker
            # for dct in [
            #     working_days, worker_holiday, missing_days, empty_days, free_day_complete_cycle,
            #     contract_type, c2d, c3d, l_d, l_q, t_lq, data_admissao, data_demissao,
            #     first_day, last_day, total_l, total_l_dom, fixed_days_off, fixed_LQs, role_by_worker
            # ]:
            #     if isinstance(dct, dict):
            #         dct.pop(DROP_W, None)

            # # 3) mapas (w, week, ...) → limpar chaves desse worker
            # worker_week_shift = {k: v for k, v in worker_week_shift.items() if k[0] != DROP_W}
            
            # =================================================================
            # CREATE MODEL AND DECISION VARIABLES
            # =================================================================
            self.logger.info("Creating SALSA model and decision variables")
            
            model = cp_model.CpModel()
            self.model = model
            
            logger.info(f"workers_complete: {workers_complete}")
            # Create decision variables
            shift = decision_variables(model, days_of_year, workers_complete, shifts, first_day, last_day, worker_holiday, missing_days, empty_days, closed_holidays, fixed_days_off, fixed_LQs, start_weekday)
            
            self.logger.info("Decision variables created for SALSA")
            
            # =================================================================
            # APPLY ALL SALSA CONSTRAINTS
            # =================================================================
            self.logger.info("Applying SALSA constraints")
            
            # Basic constraint: each worker has exactly one shift per day
            shift_day_constraint(model, shift, days_of_year, workers_complete, shifts)
            
            # Week working days constraint based on contract type
            week_working_days_constraint(model, shift, week_to_days_salsa_esp, workers, working_shift, contract_type, work_days_per_week)
            
            # Maximum continuous working days constraint
            maximum_continuous_working_days(model, shift, days_of_year, workers, working_shift, max_continuous_days)
            
            LQ_attribution(model, shift, workers, working_days, c2d)      
            
            # Worker week shift assignments
            assign_week_shift(model, shift, workers, week_to_days, working_days, worker_week_shift)
            
            # Working day shifts constraint
            working_day_shifts(model, shift, workers, working_days, check_shift, workers_complete_cycle, working_shift)
            
            # SALSA specific constraints
            salsa_esp_2_consecutive_free_days(model, shift, workers, working_days)
            
            self.logger.info(f"Salsa 2 day quality weekend workers workers: {workers}, c2d: {c2d}")
            salsa_esp_2_day_quality_weekend(model, shift, workers, contract_type, working_days, sundays, c2d, F_special_day, days_of_year, closed_holidays)
            
            salsa_esp_saturday_L_constraint(model, shift, workers, working_days, start_weekday, days_of_year, worker_holiday)

            salsa_esp_2_free_days_week(model, shift, workers, week_to_days_salsa_esp, working_days, admissao_proporcional, data_admissao, data_demissao, fixed_days_off, fixed_LQs, contract_type, work_days_per_week)

            first_day_not_free(model, shift, workers, working_days, first_day, working_shift)

            free_days_special_days(model, shift, sundays, workers, working_days, total_l_dom)

            compensation_days(model, shift, workers_complete, working_days, holidays, start_weekday, week_to_days, working_shift, week_compensation_limit, fixed_days_off, fixed_LQs)
                        
            self.logger.info("All SALSA constraints applied")
            
            # =================================================================
            # SET UP OPTIMIZATION OBJECTIVE
            # =================================================================
            self.logger.info("Setting up SALSA optimization objective")

            salsa_esp_optimization(model, days_of_year, workers_complete, working_shift, shift, pessObj,
                                             working_days, closed_holidays, min_workers, week_to_days, sundays, c2d,
                                             first_day, last_day, role_by_worker, work_day_hours)  # role_by_worker)

            # =================================================================
            # SOLVE THE MODEL
            # =================================================================
            self.logger.info("Solving SALSA model")
            
            schedule_df, results = solve(model, days_of_year, workers_complete, special_days, shift, shifts, work_day_hours, 
                              output_filename=os.path.join(_config_manager.system.project_root_dir, 'data', 'output', 
                                                         f'salsa_esp_schedule_{self.process_id}.xlsx'))
            
            self.final_schedule = pd.DataFrame(schedule_df).copy()
            
    # Capture solver statistics if available
            if hasattr(model, 'solver_stats'):
                self.solver_status = model.solver_stats.get('status', 'OPTIMAL')
                self.solving_time_seconds = model.solver_stats.get('solving_time_seconds')
                self.num_branches = model.solver_stats.get('num_branches')
                self.num_conflicts = model.solver_stats.get('num_conflicts')
            
            self.logger.info("SALSA algorithm execution completed successfully")
            return schedule_df
            
        except Exception as e:
            self.logger.error(f"Error in SALSA algorithm execution: {e}", exc_info=True)
            raise

    # def format_results(self, algorithm_results: pd.DataFrame = pd.DataFrame()) -> Dict[str, Any]:
    #     """
    #     Format the SALSA algorithm results for output.
        
    #     Args:
    #         algorithm_results: Final schedule DataFrame from execute_algorithm
            
    #     Returns:
    #         Dictionary containing formatted results and metadata
    #     """
    #     try:
    #         self.logger.info("Formatting SALSA algorithm results")

    #         if algorithm_results.empty and self.final_schedule is not None:
    #             algorithm_results = self.final_schedule
            
    #         if algorithm_results.empty:
    #             self.logger.warning("No algorithm results available to format")
    #             algorithm_results = pd.DataFrame()

    #         # Calculate basic statistics
    #         total_workers = len(algorithm_results['Worker'].unique()) if 'Worker' in algorithm_results.columns else 0
    #         total_days = len(algorithm_results['Day'].unique()) if 'Day' in algorithm_results.columns else 0
    #         total_assignments = len(algorithm_results)
            
    #         # Count shift distributions
    #         shift_distribution = {}
    #         if 'Shift' in algorithm_results.columns:
    #             shift_distribution = algorithm_results['Shift'].value_counts().to_dict()
            
    #         # Format results for SALSA
    #         if not algorithm_results.empty and self.start_date and self.end_date:
    #             # Convert to wide format similar to salsa_esp algorithm
    #             formatted_schedule = self.final_schedule.copy()
                
    #             # Create date range for column names
    #             date_range = pd.date_range(start=self.start_date, end=self.end_date, freq='D')
    #             new_columns = ['Worker'] + [str(date) for date in date_range]
    #             formatted_schedule.columns = new_columns

    #             # Convert to long format for consistency
    #             melted_schedule = pd.melt(formatted_schedule, 
    #                                     id_vars=['Worker'], 
    #                                     var_name='Date', 
    #                                     value_name='Status')
    #         else:
    #             melted_schedule = algorithm_results

    #         formatted_results = {
    #             'schedule': algorithm_results,
    #             'metadata': {
    #                 'algorithm_name': self.algo_name,
    #                 'total_workers': total_workers,
    #                 'total_days': total_days,
    #                 'total_assignments': total_assignments,
    #                 'shift_distribution': shift_distribution,
    #                 'execution_timestamp': datetime.now().isoformat(),
    #                 'parameters_used': self.parameters
    #             },
    #             'formatted_schedule': melted_schedule if not algorithm_results.empty else pd.DataFrame(),
    #             'summary': {
    #                 'status': 'completed',
    #                 'message': f'Successfully scheduled {total_workers} workers over {total_days} days using SALSA algorithm'
    #             }
    #         }
            
    #         self.logger.info(f"SALSA results formatted successfully: {total_assignments} assignments created")
    #         return formatted_results
            
    #     except Exception as e:
    #         self.logger.error(f"Error in SALSA results formatting: {e}", exc_info=True)
    #         raise



# Update the format_results method:
    def format_results(self, algorithm_results: pd.DataFrame = pd.DataFrame()) -> Dict[str, Any]:
        """
        Format the SALSA algorithm results for output.
        
        Args:
            algorithm_results: Final schedule DataFrame from execute_algorithm
           
        Returns:
            Dictionary containing formatted results and metadata
        """
        try:
            if algorithm_results.empty and self.final_schedule is not None:
                algorithm_results = self.final_schedule
            
            if algorithm_results.empty:
                logger.warning("No algorithm results available to format")
                return _create_empty_results(self.algo_name, self.process_id, self.start_date, self.end_date, self.parameters)

            # Calculate comprehensive statistics
            stats = _calculate_comprehensive_stats(algorithm_results, self.start_date, self.end_date, self.data_processed)
            
            # Validate constraints
            constraint_validation = _validate_constraints(algorithm_results)
            
            # Calculate quality metrics
            quality_metrics = _calculate_quality_metrics(algorithm_results)
            
            # Format schedule for different outputs
            formatted_schedules = _format_schedules(algorithm_results, self.start_date, self.end_date)
            formatted_schedules['database_format'] = formatted_schedules['database_format'].rename(columns={'Worker': 'colaborador'})

            # Get solver status (if available)
            solver_status = getattr(self, 'solver_status', 'OPTIMAL')
            
            # Get solver attributes
            solver_attributes = {
                'solving_time_seconds': getattr(self, 'solving_time_seconds', None),
                'num_branches': getattr(self, 'num_branches', None),
                'num_conflicts': getattr(self, 'num_conflicts', None)
            }
            
            # Create comprehensive results structure
            formatted_results = {
                'core_results': {
                    'schedule': algorithm_results,
                    'formatted_schedule': formatted_schedules['database_format'],
                    'wide_format_schedule': formatted_schedules['wide_format'],
                    'status': solver_status
                },
                'metadata': _create_metadata(self.algo_name, self.process_id, self.start_date, self.end_date, self.parameters, stats, solver_attributes),
                'scheduling_stats': stats,
                'constraint_validation': constraint_validation,
                'quality_metrics': quality_metrics,
                'validation': _validate_solution(algorithm_results),
                'export_info': _create_export_info(self.process_id, _config_manager.system.project_root_dir),
                'summary': {
                    'status': 'completed',
                    'message': f'Successfully scheduled {stats["workers"]["total_workers"]} workers over {stats["time_coverage"]["total_days"]} days using SALSA algorithm',
                    'key_metrics': {
                        'total_assignments': stats['shifts']['total_assignments'],
                        'coverage_percentage': stats['time_coverage']['coverage_percentage'],
                        'constraint_satisfaction': constraint_validation.get('overall_satisfaction', 100)
                    }
                }
            }

            #logger.info(f"DEBUG: formatted schedule: {formatted_results['core_results']['formatted_schedule'].shape}")
            
            self.logger.info("Enhanced SALSA results formatted successfully")
            return formatted_results
            
        except Exception as e:
            self.logger.error(f"Error in enhanced SALSA results formatting: {e}", exc_info=True)
            raise

