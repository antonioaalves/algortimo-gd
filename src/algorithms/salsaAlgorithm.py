"""File containing the SalsaAlgorithm class"""

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
from src.algorithms.model_salsa.variables import decision_variables
from src.algorithms.model_salsa.salsa_constraints import (
    free_days_special_days, shift_day_constraint, week_working_days_constraint, maximum_continuous_working_days,
    LQ_attribution, compensation_days, assign_week_shift, working_day_shifts, salsa_2_consecutive_free_days,
    salsa_2_day_quality_weekend, salsa_saturday_L_constraint, salsa_2_free_days_week, first_day_not_free,
    free_days_special_days
)
from src.algorithms.model_salsa.optimization_salsa import salsa_optimization
from src.algorithms.solver.solver import solve

from src.algorithms.helpers_algorithm import (_convert_free_days, _create_empty_results, _calculate_comprehensive_stats, 
                        _validate_constraints, _calculate_quality_metrics, 
                        _format_schedules, _create_metadata, _validate_solution, 
                        _create_export_info)


# Set up logger
logger = get_logger(PROJECT_NAME)

class SalsaAlgorithm(BaseAlgorithm):
    """
    SALSA shift scheduling algorithm implementation.

    This algorithm implements a constraint programming approach for shift scheduling:
    1. Adapt data: Read and process input DataFrames (calendario, estimativas, colaborador)
    2. Execute algorithm: Solve scheduling problem with SALSA-specific constraints
    3. Format results: Return final schedule DataFrame

    The algorithm uses OR-Tools CP-SAT solver to optimize shift assignments while respecting
    worker contracts, labor laws, and SALSA-specific operational requirements.
    """

    def __init__(self, parameters=None, algo_name: str = 'salsa_algorithm', project_name: str = PROJECT_NAME, process_id: int = 0, start_date: str = '', end_date: str = ''):
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
        # Default parameters for the SALSA algorithm
        default_parameters = {
            "shifts": ["M", "T", "L", "LQ", 'LD', "F", "A", "V", "-"],
            "check_shifts": ['M', 'T', 'L', 'LQ', 'LD'],
            "working_shifts": ['M', 'T', 'LD'],
            "real_working_shifts": ['M', 'T'],
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
            from src.algorithms.model_salsa.read_salsa import read_data_salsa
            
            processed_data = read_data_salsa(medium_dataframes, algorithm_treatment_params)
            
            
            # =================================================================
            # 4. UNPACK AND VALIDATE PROCESSED DATA
            # =================================================================
            self.logger.info("Unpacking processed data")
            
            self.logger.info("Using dict returned by read_data_salsa()")
            if not isinstance(processed_data, dict):
                raise TypeError("read_data_salsa must return a dict with named keys")
            data_dict = processed_data

            """ except IndexError as e:
                self.logger.error(f"Error unpacking processed data: {e}")
                raise ValueError(f"Invalid data structure returned from processing function: {e}") """
            
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
            worker_absences = adapted_data['worker_absences']
            vacation_days = adapted_data['vacation_days']
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
            week_to_days_salsa = adapted_data['week_to_days_salsa']
            first_day = adapted_data['first_registered_day']
            admissao_proporcional = adapted_data['admissao_proporcional']
            data_admissao = adapted_data['data_admissao']
            data_demissao = adapted_data['data_demissao']
            last_day = adapted_data['last_registered_day']
            fixed_days_off = adapted_data['fixed_days_off']
            fixed_LQs = adapted_data['fixed_LQs']
            fixed_M = adapted_data['fixed_M']
            fixed_T = adapted_data['fixed_T']
            role_by_worker = adapted_data['role_by_worker']
            #managers = adapted_data['managers']
            #keyholders = adapted_data['keyholders']
            # week_cut = adapted_data['week_cut']
            proportion = adapted_data['proportion']
            work_day_hours = adapted_data['work_day_hours']
            work_days_per_week = adapted_data['work_days_per_week']
            week_compensation_limit = adapted_data['week_compensation_limit']
            max_continuous_days = adapted_data["num_dias_cons"]
            country = adapted_data["country"]
            partial_workers_complete = adapted_data['partial_workers_complete']
            workers_past = adapted_data['workers_past']
            fixed_compensation_days = adapted_data['fixed_compensation_days']

            # Extract algorithm parameters
            shifts = self.parameters["shifts"]
            check_shift = self.parameters["check_shifts"]
            working_shift = self.parameters["working_shifts"]
            real_working_shift = self.parameters["real_working_shifts"]
            
            if country != "spain":
                shifts.remove("LD")
                check_shift.remove("LD")
                working_shift.remove("LD")
                if max_continuous_days == None:
                    max_continuous_days = 6

            # Extract settings
            settings = self.parameters["settings"]
            F_special_day = settings["F_special_day"]
            free_sundays_plus_c2d = settings["free_sundays_plus_c2d"]
            missing_days_afect_free_days = settings["missing_days_afect_free_days"]


            logger.info(f"Valid workers after processing: {workers}")
            logger.info(f"Valid past workers after processing: {workers_past}")

            # === TEST: remover totalmente um worker problemático ===
            # DROP_WORKERS = [ 80001244, 1940, 2599, 80000907, 222, 111]  # Add more worker IDs as needed
            # logger.warning(f"[TEST] Dropping workers {DROP_WORKERS} for feasibility test")

            # # 1) listas de workers
            # workers = [w for w in workers if w not in DROP_WORKERS]
            # workers_complete = [w for w in workers_complete if w not in DROP_WORKERS]
            # workers_complete_cycle = [w for w in workers_complete_cycle if w not in DROP_WORKERS]

            # # 2) dicionários por worker
            # for dct in [
            #     working_days, worker_absences, missing_days, empty_days, free_day_complete_cycle,
            #     contract_type, c2d, c3d, l_d, l_q, t_lq, data_admissao, data_demissao,
            #     first_day, last_day, total_l, total_l_dom, fixed_days_off, fixed_LQs, role_by_worker
            # ]:
            #     if isinstance(dct, dict):
            #         for worker_id in DROP_WORKERS:
            #             dct.pop(worker_id, None)

            # # 3) mapas (w, week, ...) → limpar chaves desses workers
            # worker_week_shift = {k: v for k, v in worker_week_shift.items() if k[0] not in DROP_WORKERS}

            # =================================================================
            # CREATE MODEL AND DECISION VARIABLES
            # =================================================================
            self.logger.info("Creating SALSA model and decision variables")
            
            model = cp_model.CpModel()
            self.model = model
            
            # Create decision variables
            shift = decision_variables(model, workers_complete, shifts, first_day, last_day, worker_absences,
                                       vacation_days, empty_days, closed_holidays, fixed_days_off, fixed_LQs, 
                                       fixed_M, fixed_T, start_weekday, workers_past, fixed_compensation_days)
            
            self.logger.info("Decision variables created for SALSA")
            
            # =================================================================
            # APPLY ALL SALSA CONSTRAINTS
            # =================================================================
            self.logger.info("Applying SALSA constraints")
            
            # Basic constraint: each worker has exactly one shift per day
            shift_day_constraint(model, shift, days_of_year, workers_complete, shifts)
            
            # Week working days constraint based on contract type
            week_working_days_constraint(model, shift, week_to_days_salsa, workers, working_shift, contract_type, work_days_per_week)
            
            # Maximum continuous working days constraint
            maximum_continuous_working_days(model, shift, days_of_year, workers, working_shift, max_continuous_days)
            
            LQ_attribution(model, shift, workers, working_days, c2d)           
            
            # Worker week shift assignments
            assign_week_shift(model, shift, workers, week_to_days, working_days, worker_week_shift)
            
            # Working day shifts constraint
            working_day_shifts(model, shift, workers, working_days, check_shift, workers_complete_cycle, working_shift)
            
            # SALSA specific constraints
            salsa_2_consecutive_free_days(model, shift, workers, working_days, contract_type, fixed_days_off, fixed_LQs)
            
            self.logger.info(f"Salsa 2 day quality weekend workers workers: {workers}, c2d: {c2d}")
            salsa_2_day_quality_weekend(model, shift, workers, contract_type, working_days, sundays, c2d, F_special_day, days_of_year, closed_holidays)
            
            salsa_saturday_L_constraint(model, shift, workers, working_days, start_weekday)

            salsa_2_free_days_week(model, shift, workers, week_to_days_salsa, working_days, admissao_proporcional, data_admissao, data_demissao, fixed_days_off, fixed_LQs, contract_type, work_days_per_week)

            first_day_not_free(model, shift, workers, working_days, first_day, working_shift, fixed_days_off)

            free_days_special_days(model, shift, sundays, workers, working_days, total_l_dom)

            if country == "spain":
                compensation_days(model, shift, workers_complete, working_days, holidays, week_to_days, real_working_shift, week_compensation_limit, fixed_days_off, fixed_LQs, worker_absences, vacation_days)
                        
            self.logger.info("All SALSA constraints applied")
            
            # =================================================================
            # SET UP OPTIMIZATION OBJECTIVE
            # =================================================================
            self.logger.info("Setting up SALSA optimization objective")

            debug_vars, optimization_details = salsa_optimization(model, days_of_year, workers_complete, working_shift, shift, pessObj,
                                             working_days, closed_holidays, min_workers, week_to_days, sundays, c2d,
                                             first_day, last_day, role_by_worker, work_day_hours, workers_past)  # role_by_worker)

            # =================================================================
            # SOLVE THE MODEL
            # =================================================================
            self.logger.info("Solving SALSA model")
            
            schedule_df, results = solve(model, days_of_year, workers_complete, special_days, shift, shifts, work_day_hours, pessObj, workers_past,
                              output_filename=os.path.join(ROOT_DIR, 'data', 'output', f'salsa_schedule_{self.process_id}.xlsx'),
                              optimization_details=optimization_details )
            self.final_schedule = pd.DataFrame(schedule_df).copy()
            logger.info(f"Final schedule shape: {self.final_schedule.shape}")

            # =================================================================
            # FILTER BY PARTIAL WORKERS IF REQUESTED
            # =================================================================
            # Check if we need to filter by partial workers
            if partial_workers_complete and len(partial_workers_complete) > 0:
                logger.info(f"Filtering schedule by partial_workers_complete: {partial_workers_complete}")
                
                if isinstance(schedule_df, pd.DataFrame) and not schedule_df.empty:
                    # Keep the first row (header/metadata) and rows for specific workers
                    if 'Worker' in schedule_df.columns:
                        # Filter to keep first row and rows with workers in partial_workers_complete
                        first_row = schedule_df.iloc[:1]  # First row
                        worker_rows = schedule_df[schedule_df['Worker'].isin(partial_workers_complete)]
                        
                        # Combine first row with filtered worker rows
                        schedule_df = pd.concat([first_row, worker_rows], ignore_index=True)
                        
                        logger.info(f"Filtered schedule: kept first row + {len(worker_rows)} worker rows for workers {partial_workers_complete}")
                    
                    elif 'colaborador' in schedule_df.columns:
                        # Alternative column name
                        first_row = schedule_df.iloc[:1]  # First row
                        worker_rows = schedule_df[schedule_df['colaborador'].isin(partial_workers_complete)]
                        
                        # Combine first row with filtered worker rows
                        schedule_df = pd.concat([first_row, worker_rows], ignore_index=True)
                        
                        logger.info(f"Filtered schedule: kept first row + {len(worker_rows)} worker rows for workers {partial_workers_complete}")
                    
                    else:
                        # If no worker column found, try to filter by index or other method
                        logger.warning("No 'Worker' or 'colaborador' column found. Cannot filter by partial workers.")
                
                else:
                    logger.warning("Schedule DataFrame is empty or not a DataFrame. Cannot filter by partial workers.")

            else:
                logger.info("No partial workers specified or partial_workers_complete is empty. Using full schedule.")

            
            # Log comprehensive optimization analysis
            logger.info("\n=== OPTIMIZATION ANALYSIS ===")
            if results is not None:
                logger.info(f"Net objective value: {results['summary']['net_objective']}")
                
                logger.info("--- Point-by-point breakdown ---")
                breakdown = results['summary']['point_breakdown']
                
                # Point 1: Pessimistic objective deviations
                if breakdown['point_1_pessobj_deviations'] > 0:
                    logger.info(f"Point 1 - PessObj deviations: {breakdown['point_1_pessobj_deviations']} penalty")
                else:
                    logger.info("Point 1 - PessObj deviations: 0 penalty (perfect worker allocation)")
                
                # Point 2: Consecutive free days bonus
                if results['point_2_consecutive_free_days']['total_bonus'] > 0:
                    logger.info(f"Point 2 - Consecutive free days: -{results['point_2_consecutive_free_days']['total_bonus']} bonus")
                else:
                    logger.info("Point 2 - Consecutive free days: 0 bonus (no consecutive free days)")
                
                # Point 3: No workers penalty
                if breakdown['point_3_no_workers'] > 0:
                    logger.info(f"Point 3 - No workers penalty: {breakdown['point_3_no_workers']} penalty")
                else:
                    logger.info("Point 3 - No workers penalty: 0 penalty (all shifts properly covered)")
                
                # Point 4: Minimum workers penalty
                if breakdown['point_4_1_min_workers'] > 0:
                    logger.info(f"Point 4.1 - Minimum workers penalty: {breakdown['point_4_1_min_workers']} penalty")
                else:
                    logger.info("Point 4.1 - Minimum workers penalty: 0 penalty (all minimum requirements met)")

                if breakdown['point_4_2_min_workers'] > 0:
                    logger.info(f"Point 4.2 - Minimum workers penalty: {breakdown['point_4_2_min_workers']} penalty")
                else:
                    logger.info("Point 4.2 - Minimum workers penalty: 0 penalty (all minimum requirements met)")
                
                # Point 5.1: Sunday balance penalty
                if breakdown['point_5_1_sunday_balance'] > 0:
                    logger.info(f"Point 5.1 - Sunday balance penalty: {breakdown['point_5_1_sunday_balance']} penalty")
                else:
                    logger.info("Point 5.1 - Sunday balance penalty: 0 penalty (even Sunday distribution per worker)")
                
                # Point 5.2: C2D balance penalty
                if breakdown['point_5_2_c2d_balance'] > 0:
                    logger.info(f"Point 5.2 - C2D balance penalty: {breakdown['point_5_2_c2d_balance']} penalty")
                else:
                    logger.info("Point 5.2 - C2D balance penalty: 0 penalty (even quality weekend distribution per worker)")
                
                # Point 6: Inconsistent shifts penalty
                if breakdown['point_6_inconsistent_shifts'] > 0:
                    logger.info(f"Point 6 - Inconsistent shifts penalty: {breakdown['point_6_inconsistent_shifts']} penalty")
                else:
                    logger.info("Point 6 - Inconsistent shifts penalty: 0 penalty (consistent shift types per worker per week)")
                
                # Point 7: Sunday balance across workers
                if breakdown['point_7_sunday_balance_across_workers'] > 0:
                    logger.info(f"Point 7.1 - Sunday balance across workers: {breakdown['point_7_sunday_balance_across_workers']} penalty")
                else:
                    logger.info("Point 7.1 - Sunday balance across workers: 0 penalty (proportional Sunday distribution)")
                
                # Point 7B: LQ balance across workers
                if breakdown['point_7b_lq_balance_across_workers'] > 0:
                    logger.info(f"Point 7.2 - LQ balance across workers: {breakdown['point_7b_lq_balance_across_workers']} penalty")
                else:
                    logger.info("Point 7.2 - LQ balance across workers: 0 penalty (proportional quality weekend distribution)")
                
                # Point 8: Manager/Keyholder conflicts
                if breakdown['point_8_manager_keyholder_conflicts'] > 0:
                    logger.info(f"Point 8 - Manager/Keyholder conflicts: {breakdown['point_8_manager_keyholder_conflicts']} penalty")
                else:
                    logger.info("Point 8 - Manager/Keyholder conflicts: 0 penalty (no scheduling conflicts)")
                
            logger.info("=== END OPTIMIZATION ANALYSIS ===\n")

            
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

   
# Update the format_results method:
    def format_results(self, algorithm_results: pd.DataFrame = pd.DataFrame(), week_to_days_salsa : Dict[int, List[int]] = None) -> Dict[str, Any]:
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

            # Convert free days codes in wfm codes FO and FC
            algorithm_results = _convert_free_days(algorithm_results, self.data_processed)

            logger.info(f"DEBUG: schedule after convert: {algorithm_results.head(5)}")
            
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
                'export_info': _create_export_info(self.process_id, ROOT_DIR),
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

