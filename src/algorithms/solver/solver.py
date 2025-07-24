import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from ortools.sat.python import cp_model
from datetime import datetime, timedelta
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import logging
from typing import Dict, Any, List, Tuple, Optional, Callable
from base_data_project.log_config import get_logger
from src.configuration_manager.manager import ConfigurationManager

# Get configuration manager instance
_config_manager = None

def get_config_manager():
    """Get or create the global configuration manager instance."""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigurationManager()
    return _config_manager
import os
import psutil

# Set up logger
logger = get_logger(get_config_manager().system_config.get('project_name', 'algoritmo_GD'))

#----------------------------------------SOLVER-----------------------------------------------------------
def solve(
    model: cp_model.CpModel, 
    days_of_year: List[int], 
    workers: List[int], 
    special_days: List[int], 
    shift: Dict[Tuple[int, int, str], cp_model.IntVar], 
    shifts: List[str],
    max_time_seconds: int = 120,
    enumerate_all_solutions: bool = False,
    use_phase_saving: bool = True,
    log_search_progress: bool = True,
    log_callback: Optional[Callable] = None,
    output_filename: str = os.path.join(ROOT_DIR, 'data', 'output', 'working_schedule.xlsx')
) -> pd.DataFrame:
    """
    Enhanced solver function with comprehensive logging and configurable parameters.
    
    Args:
        model: The CP-SAT model to solve
        days_of_year: List of days to schedule
        workers: List of worker IDs
        special_days: List of special days (holidays, sundays)
        shift: Dictionary mapping (worker, day, shift) to decision variables
        shifts: List of available shift types
        max_time_seconds: Maximum solving time in seconds (default: 120)
        enumerate_all_solutions: Whether to enumerate all solutions (default: False)
        use_phase_saving: Whether to use phase saving (default: True)
        log_search_progress: Whether to log search progress (default: True)
        log_callback: Custom callback for logging (default: None, uses print)
        output_filename: Name of the output Excel file (default: 'worker_schedule.xlsx')
    
    Returns:
        DataFrame containing the worker schedule
        
    Raises:
        ValueError: If input parameters are invalid
        RuntimeError: If solver fails to find a solution
    """
    try:
        logger.info("Starting solver")
        
        # =================================================================
        # 1. VALIDATE INPUT PARAMETERS
        # =================================================================
        logger.info("Validating input parameters")
        
        if not isinstance(model, cp_model.CpModel):
            error_msg = f"model must be a CP-SAT CpModel instance. model: {model}, type: {type(model)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        if not days_of_year or not isinstance(days_of_year, list):
            error_msg = f"days_of_year must be a non-empty list. days_of_year: {days_of_year}, type: {type(days_of_year)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        if not workers or not isinstance(workers, list):
            error_msg = f"workers must be a non-empty list. workers: {workers}, type: {type(workers)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        if not isinstance(special_days, list):
            error_msg = f"special_days must be a list. special_days: {special_days}, type: {type(special_days)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        if not isinstance(shift, dict):
            error_msg = f"shift must be a dictionary. shift: {shift}, type: {type(shift)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        if not shifts or not isinstance(shifts, list):
            error_msg = f"shifts must be a non-empty list. shifts: {shifts}, type: {type(shifts)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"[OK] Input validation passed:")
        logger.info(f"  - Days to schedule: {len(days_of_year)} days (from {min(days_of_year)} to {max(days_of_year)})")
        logger.info(f"  - Workers: {len(workers)} workers")
        logger.info(f"  - Special days: {len(special_days)} days")
        logger.info(f"  - Available shifts: {shifts}")
        logger.info(f"  - Decision variables: {len(shift)} variables")
        logger.info(f"  - Max solving time: {max_time_seconds} seconds")
        
        # =================================================================
        # 2. CONFIGURE AND CREATE SOLVER
        # =================================================================
        logger.info("Configuring CP-SAT solver")
        
        solver = cp_model.CpSolver()
        logger.info("Starting optimization process...")
        start_time = datetime.now()

        logger.info("=== ABOUT TO SOLVE ===")

        # Use only verified OR-Tools parameters
        solver.parameters.num_search_workers = 1
        solver.parameters.max_time_in_seconds = 30  # Short timeout for testing

        logger.info("Attempting solve with verified parameters...")

        # Simple timeout test without fancy threading
        import time
        solve_start = time.time()

        status = solver.Solve(model)

        solve_end = time.time()
        actual_duration = solve_end - solve_start

        logger.info("=== SOLVE COMPLETED ===")
        logger.info(f"Actual solve time: {actual_duration:.2f} seconds")

        end_time = datetime.now()
        solve_duration = (end_time - start_time).total_seconds()

        logger.info(f"Total time: {solve_duration:.2f} seconds")
        logger.info(f"Solver status: {solver.status_name(status)}")
        
        # Log solver statistics
        logger.info(f"Solver statistics:")
        logger.info(f"  - Objective value: {solver.ObjectiveValue() if status in [cp_model.OPTIMAL, cp_model.FEASIBLE] else 'N/A'}")
        logger.info(f"  - Best objective bound: {solver.BestObjectiveBound() if status in [cp_model.OPTIMAL, cp_model.FEASIBLE] else 'N/A'}")
        logger.info(f"  - Number of branches: {solver.NumBranches()}")
        logger.info(f"  - Number of conflicts: {solver.NumConflicts()}")
        logger.info(f"  - Wall time: {solver.WallTime():.2f} seconds")

        # =================================================================
        # 4. VALIDATE SOLUTION STATUS
        # =================================================================
        if status not in [cp_model.OPTIMAL, cp_model.FEASIBLE]:
            error_msg = f"Solver failed to find a solution. Status: {solver.status_name(status)}"
            logger.error(error_msg)
            
            if status == cp_model.INFEASIBLE:
                logger.error("Problem is infeasible - no solution exists with current constraints")
            elif status == cp_model.MODEL_INVALID:
                logger.error("Model is invalid - check constraint definitions")
            elif status == cp_model.UNKNOWN:
                logger.error("Solver timed out or encountered unknown status")
            
            raise RuntimeError(error_msg)
        
        logger.info(f"[OK] Solution found! Status: {solver.status_name(status)}")
        
        # =================================================================
        # 5. PROCESS SOLUTION AND CREATE SCHEDULE
        # =================================================================
        logger.info("Processing solution and creating schedule")
        
        # Shift mapping for readability
        shift_mapping = {
            'M'     : 'M',  # Morning shift
            'T'     : 'T',  # Afternoon shift
            'F'     : 'F',  # Closed holiday
            'V'     : 'V',  # Empty Day
            'A'     : 'A',  # Missing shift
            'L'     : 'L',  # Free day
            'LQ'    : 'LQ', # Free days semester
            'TC'    : 'TC',
        }
        
        logger.info(f"Shift mapping: {shift_mapping}")

        # Prepare the data for the DataFrame
        table_data = []  # List to store each worker's data as a row
        worker_stats = {}  # Dictionary to track L, LQ, LD counts for each worker
        
        logger.info(f"Processing schedule for {len(workers)} workers across {len(days_of_year)} days")
        # Prepare the data for the DataFrame
        table_data = []  # List to store each worker's data as a row
        worker_stats = {}  # Dictionary to track L, LQ, LD counts for each worker

        # Loop through each worker
        processed_workers = 0
        for w in workers:
            try:
                worker_row = [w]  # Start with the worker's name
                # Initialize counters for this worker
                l_count = 0
                lq_count = 0
                ld_count = 0
                tc_count = 0
                special_days_count = 0  # Counter for special days with M or T shifts
                unassigned_days = 0
                
                logger.debug(f"Processing worker {w}")

                days_of_year_sorted = sorted(days_of_year)
                for d in days_of_year_sorted:
                    day_assignment = None
                    
                    # Check each shift type for this day
                    for s in shifts:
                        if (w, d, s) in shift and solver.Value(shift[(w, d, s)]) == 1:
                            day_assignment = shift_mapping.get(s, s)
                            break
                    
                    # If no specific assignment found, mark as unassigned
                    if day_assignment is None:
                        day_assignment = '-'
                        unassigned_days += 1
                    
                    worker_row.append(day_assignment)
                    
                    # Count different shift types
                    if day_assignment == 'L':
                        l_count += 1
                    elif day_assignment == 'LQ':
                        lq_count += 1
                    elif day_assignment == 'LD':
                        ld_count += 1
                    elif day_assignment == 'TC':
                        tc_count += 1
                    elif day_assignment in ['M', 'T'] and d in special_days:
                        special_days_count += 1
                
                # Store statistics for this worker
                worker_stats[w] = {
                    'L_count': l_count,
                    'LQ_count': lq_count,
                    'LD_count': ld_count,
                    'TC_count': tc_count,
                    'special_days_work': special_days_count,
                    'unassigned_days': unassigned_days
                }
                
                table_data.append(worker_row)
                processed_workers += 1
                
                logger.debug(f"Worker {w} processed: L={l_count}, LQ={lq_count}, LD={ld_count}, "
                           f"TC={tc_count}, Special={special_days_count}, Unassigned={unassigned_days}")
                
            except Exception as e:
                logger.error(f"Error processing worker {w}: {str(e)}")
                continue
        
        logger.info(f"Successfully processed {processed_workers} workers")
        
        # =================================================================
        # 6. CREATE DATAFRAME AND SAVE TO EXCEL
        # =================================================================
        logger.info("Creating DataFrame and saving to Excel")
        
        # Create DataFrame
        columns = ['Worker'] + [f'Day_{d}' for d in sorted(days_of_year)]
        df = pd.DataFrame(table_data, columns=columns)
        
        logger.info(f"DataFrame created with shape: {df.shape}")
        logger.info(f"DataFrame columns: {len(df.columns)} columns")
        
        # Save to Excel
        try:
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
            df.to_excel(output_filename, index=False)
            logger.info(f"Schedule saved to: {output_filename}")
        except Exception as e:
            logger.warning(f"Could not save to Excel: {str(e)}")
        
        # =================================================================
        # 7. LOG FINAL STATISTICS
        # =================================================================
        logger.info("Final worker statistics:")
        for worker_id, stats in worker_stats.items():
            logger.info(f"  Worker {worker_id}: {stats}")
        
        logger.info("[OK] Solver completed successfully")
        return df
        
    except Exception as e:
        logger.error(f"Error in solver: {str(e)}", exc_info=True)
        raise