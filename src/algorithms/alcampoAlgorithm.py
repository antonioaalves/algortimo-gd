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
from src.configuration_manager.instance import get_config as get_config_manager

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

from src.helpers import (_create_empty_results, _calculate_comprehensive_stats, 
                        _validate_constraints, _calculate_quality_metrics, 
                        _format_schedules, _create_metadata, _validate_solution, 
                        _create_export_info)


from src.helpers import (_create_empty_results, _calculate_comprehensive_stats, 
                        _validate_constraints, _calculate_quality_metrics, 
                        _format_schedules, _create_metadata, _validate_solution, 
                        _create_export_info)


# Initialize logger with project name from config
logger = get_logger(get_config_manager().system.project_name)
root_dir = get_config_manager().system.project_root_dir

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
        """
        # Default parameters for the algorithm
        default_parameters = {
            "shifts": ['M', 'T', 'L', 'LQ', 'F', 'V', 'LD', 'A', 'TC', '-'],
            "check_shifts": ['M', 'T', 'L', 'LQ', "LD", "TC"],
            "check_shift_special": ['M', 'T', 'L', "TC"],
            "working_shifts": ['M', 'T', 'TC'],
            "real_working_shifts": ['M', 'T'],
            "max_continuous_working_days": 10,

            "settings":{
                #F days affect c2d and cxx
                "F_special_day": False,
                #defines if we should sum 2 day quality weekends with the number of free sundays
                "free_sundays_plus_c2d": False,
                "vacation_days_afect_free_days": False,
            }
        }
        
        # Merge with provided parameters
        if parameters:
            default_parameters.update(parameters)
        
        # Validate algorithm is available
        if project_name is None:
            project_name = get_config_manager().system.project_name
        
        # Store algorithm configuration
        self.algo_name = algo_name
        self.project_name = project_name
        self.process_id = process_id
        self.start_date = start_date
        self.end_date = end_date
        # Initialize parent class
        super().__init__(algo_name=algo_name, parameters=default_parameters, project_name=project_name)

        
        self.logger.info(f"Algorithm {algo_name} initialized successfully")
        self.logger.debug(f"Parameters: {self.parameters}")

    def adapt_data(self, data: Dict[str, pd.DataFrame], algorithm_treatment_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Adapt data for the Alcampo algorithm.
        
        Args:
            data: Dictionary containing DataFrames:
                - Should contain medium_dataframes with 'matrizA_bk', 'matrizB_bk', 'matriz2_bk'
            algorithm_treatment_params: Optional dictionary containing algorithm-specific
                                      data treatment parameters (ignored by AlcampoAlgorithm)
                
        Returns:
            Adapted data ready for algorithm processing
        """
        try:
            self.logger.info("Starting data adaptation for Alcampo algorithm")
            
            # AlcampoAlgorithm doesn't use treatment parameters, but accepts them for interface consistency
            if algorithm_treatment_params:
                self.logger.debug(f"AlcampoAlgorithm received treatment parameters but ignores them: {list(algorithm_treatment_params.keys())}")
            
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
            data_dict = processed_data
            
            # =================================================================
            # 4. UNPACK AND VALIDATE PROCESSED DATA
            # =================================================================
            self.logger.info("Unpacking processed data")
            
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
            worker_absences = adapted_data['worker_absences']
            vacation_days = adapted_data['vacation_days']
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
            pessObj = adapted_data['pess_obj']
            min_workers = adapted_data['min_workers']
            max_workers = adapted_data['max_workers']
            workers_complete = adapted_data['workers_complete']
            workers_complete_cycle = adapted_data['workers_complete_cycle']
            first_day = adapted_data['first_registered_day']
            last_day = adapted_data['last_registered_day']
            fixed_days_off = adapted_data['fixed_days_off']
            workers_past = adapted_data["workers_past"]
            period = adapted_data["period"]
            dummy_workers = adapted_data["dummy_workers"]
            workers_with_dummy = adapted_data["workers_with_dummy"]
            unique_dates = adapted_data["unique_dates"]
            index_to_date = adapted_data["index_to_date"]
            fixed_LQs = adapted_data["fixed_LQs"]
            shift_data = adapted_data["shift_data"]
            fixed_compensation_days = adapted_data["fixed_compensation_days"]
            locked_days = adapted_data["locked_days"]
            forced_work_days = adapted_data["forced_work_days"]
            complete_cycle_days = adapted_data["complete_cycle_days"]

            # Extract algorithm parameters
            shifts = self.parameters["shifts"]
            check_shift = self.parameters["check_shifts"]
            check_shift_special = self.parameters["check_shift_special"]
            working_shift = self.parameters["working_shifts"]
            real_working_shift = self.parameters["real_working_shift"]
            max_continuous_days = self.parameters["max_continuous_working_days"]
            
            # =================================================================
            # STAGE 1: Initial scheduling with all constraints
            # =================================================================

            self.logger.info("Starting Stage 1: Initial scheduling")
            
            model = cp_model.CpModel()
            self.model_stage1 = model
            
            self.logger.info("Model initialized for Stage 1")

            # Create decision variables
            shift = decision_variables(model, workers_complete, shifts, first_day, last_day, worker_absences, vacation_days, empty_days, 
                                       closed_holidays, fixed_days_off, fixed_LQs, shift_data, workers_past, fixed_compensation_days,
                                       locked_days, forced_work_days, contract_type, complete_cycle_days, real_working_shift)
            self.logger.info("Decision variables created for Stage 1")
            
            # Apply all constraints
            self._apply_stage1_constraints(
                                 model, shift, days_of_year, workers, shifts, check_shift, 
                                 check_shift_special, working_shift, max_continuous_days, week_to_days,
                                 real_working_shift, contract_type, special_days, total_l, c2d, c3d, working_days,
                                 total_l_dom, tc, l_d, l_q, cxx, closed_holidays, worker_absences,
                                 vacation_days, empty_days, worker_week_shift, start_weekday, sundays,
                                 t_lq, matriz_calendario_gd, workers_complete, workers_complete_cycle,)

            self.logger.info("Constraints applied for Stage 1")
            
            # Set up optimization objective
            debug_vars = optimization_prediction(
                model, days_of_year, workers_complete, workers_complete_cycle, working_shift, shift, pessObj, 
                min_workers, closed_holidays, week_to_days, working_days, contract_type, special_days
            )
            
            logger.info(f"Optimization variables for Stage 1: {debug_vars}")

            # Solve Stage 1
            self.logger.info("Solving Stage 1 model")
            schedule_df = solve(model, days_of_year, workers_complete, special_days, shift, shifts, self.process_id, output_filename=os.path.join(root_dir, 'data', 'output', f'working_schedule_{self.process_id}-stage1.xlsx'),
                                debug_vars=debug_vars)
            work_day_hours = {}
            h_plus = {}
            eci_sibling_results_flag = False
            schedule_df, feriados_domingos_compensacao = solve(model, days_of_year, workers_complete, sundays, holidays, shift, shifts, real_working_shift, work_day_hours, pessObj,
                                                     workers_past, h_plus, contingente_f, contingente_d, eci_sibling_results_flag, period, index_to_date, dummy_workers, workers_with_dummy,
                                                     pd.Series(['Worker'] + (unique_dates)),
                                                     output_filename=os.path.join(root_dir, 'data', 'output', f'salsa_schedule_{self.process_id}.xlsx'))
                        
            self.schedule_stage1 = pd.DataFrame(schedule_df).copy()
            
            # =================================================================
            # STAGE 2: Refinement with 3-day weekend constraints
            # =================================================================
            self.logger.info("Starting Stage 2: Schedule refinement")
            
            new_model = cp_model.CpModel()
            self.model_stage2 = new_model
            
            # Create new decision variables
            new_shift = decision_variables(new_model, days_of_year, workers, shifts, first_day, last_day, worker_absences, vacation_days, empty_days, closed_holidays, fixed_days_off, start_weekday)
            
            # Apply Stage 2 constraints
            self._apply_stage2_constraints(new_model, new_shift, days_of_year, workers, shifts,
                                 total_l, working_days, l_q, c2d, c3d, schedule_df, start_weekday,
                                 contract_type, closed_holidays, workers_complete, workers_complete_cycle)
            
            # Apply optimization (reusing from Stage 1)
            debug_vars = optimization_prediction(
                new_model, days_of_year, workers_complete, workers_complete_cycle,working_shift, new_shift, pessObj,
                min_workers, closed_holidays, week_to_days, working_days, contract_type, special_days
            )
            
            
            # Solve Stage 2
            self.logger.info("Solving Stage 2 model")
            final_schedule_df = solve(new_model, days_of_year, workers_complete, special_days, new_shift, shifts, self.process_id, output_filename=os.path.join(root_dir, 'data', 'output', f'working_schedule_{self.process_id}-stage2.xlsx'),
                                       debug_vars=debug_vars)
            #final_schedule_df = solve_alcampo(adapted_data, shifts, check_shift, check_shift_special, working_shift, max_continuous_days)
            self.final_schedule = pd.DataFrame(final_schedule_df).copy()
            
            self.logger.info("Alcampo algorithm execution completed successfully")
            self.logger.info(f"Final schedule for Stage 2: {final_schedule_df}")
            return final_schedule_df
            
        except Exception as e:
            self.logger.error(f"Error in algorithm execution: {e}", exc_info=True)
            raise

    def _apply_stage1_constraints(self, model, shift, days_of_year, workers, shifts, check_shift, 
                                 check_shift_special, working_shift, max_continuous_days, week_to_days,
                                 real_working_shift, contract_type, special_days, total_l, c2d, c3d, working_days,
                                 total_l_dom, tc, l_d, l_q, cxx, closed_holidays, worker_absences,
                                 vacation_days, empty_days, worker_week_shift, start_weekday, sundays,
                                 t_lq, matriz_calendario_gd, workers_complete, workers_complete_cycle):
        """Apply all Stage 1 constraints to the model."""
        

        shift_day_constraint(model, shift, days_of_year, workers_complete, shifts)
        
        # Constraint to limit working days in a week based on contract type
        week_working_days_constraint(model, shift, week_to_days, workers, working_shift, contract_type)
        
        # Constraint to limit maximum continuous working days
        maximum_continuous_working_days(model, shift, days_of_year, workers, working_shift, max_continuous_days)
        
        # Constraint to limit maximum continuous working special days
        maximum_continuous_working_special_days(model, shift, special_days, workers, working_shift, contract_type)
        
        # Constraint to limit maximum free days in a year
        maximum_free_days(model, shift, days_of_year, workers, total_l, c3d)
        
        # Constraint for free days on special days
        free_days_special_days(model, shift, special_days, workers, working_days, total_l_dom)
        
        # TC attribution constraint
        tc_atribution(model, shift, workers, tc, special_days, working_days)
        
        # Working days special days constraint
        working_days_special_days(model, shift, special_days, workers, working_days, l_d, contract_type)
        
        # LQ attribution constraint
        LQ_attribution(model, shift, workers, working_days, l_q, c2d)
        
        # LD attribution constraint
        LD_attribution(model, shift, workers, working_days, l_d)

        #closed_holiday_attribution(model, shift, workers_complete, closed_holidays)

        #holiday_missing_day_attribution(model, shift, workers_complete, worker_absences, vacation_days, empty_days, free_day_complete_cycle)
        
        # Worker week shift assignments #####
        assign_week_shift(model, shift, workers_complete, week_to_days, working_days, worker_week_shift)
        
        # Working day shifts constraint
        # for w in workers:
        #     logger.info(f"Applying working day shifts constraint for worker {w}: shifts: {working_shift} and check shifts: {check_shift}")  
        #     logger.info(f"Worker {w}, working days: {working_days[w]}, special days: {special_days}, \
        #                 worker empty days: {empty_days[w]}, worker missing days: {vacation_days[w]}, worker holiday: {worker_absences[w]} \
        #                 fixed free days: {free_day_complete_cycle[w]}")
        working_day_shifts(model, shift, workers, working_days, check_shift)
        
        # Special day shifts constraint 
        special_day_shifts(model, shift, workers, special_days, check_shift_special, working_days)

        # Complete cycle shifts constraint
        complete_cycle_shifts(model, shift, workers_complete_cycle, working_days, real_working_shift)
        
        # Free days adjacent to weekends
        free_day_next_2c(model, shift, workers, working_days, start_weekday, closed_holidays)
        
        # Limit consecutive free days during the week
        no_free__days_close(model, shift, workers, working_days, start_weekday, week_to_days, cxx, contract_type, closed_holidays, days_of_year)
        
        # Day2 quality weekends
        day2_quality_weekend(model, shift, workers, working_days, sundays, c2d, contract_type, closed_holidays)
        
        # Space LQs constraint
        space_LQs(model, shift, workers, working_days, t_lq, matriz_calendario_gd)
        
        # # Priority 2-3 workers constraint
        prio_2_3_workers(model, shift, workers, working_days, special_days, start_weekday, week_to_days, contract_type, working_shift)
        
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
        Format the SALSA algorithm results for output.
        
        Args:
            algorithm_results: Final schedule DataFrame from execute_algorithm
           
        Returns:
            Formatted results (DataFrame or dict)
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
                'export_info': _create_export_info(self.process_id, root_dir),
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

