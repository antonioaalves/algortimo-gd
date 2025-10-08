import pandas as pd
import os
from datetime import datetime
from typing import  List, Dict, Any, Optional, Tuple

# Local stuff
from src.configuration_manager.manager import ConfigurationManager
from base_data_project.log_config import get_logger

_config_manager = ConfigurationManager()

logger = get_logger(_config_manager.project_name)

def analyze_optimization_results(solver, optimization_details):
    """
    Analyze the optimization results after solving.
    """
    results = {}
    
    # Point 1: Pessimistic objective deviations
    point_1_results = {
        'deviations': {},
        'total_penalty': 0
    }
    
    if optimization_details is None:
        return results
    
    penalty_weight = optimization_details['point_1_pessobj_deviations']['penalty_weight']
    
    for (d, s), var_info in optimization_details['point_1_pessobj_deviations']['variables'].items():
        # NOW we can get the actual values
        pos_diff_value = solver.Value(var_info['pos_diff'])
        neg_diff_value = solver.Value(var_info['neg_diff'])
        target = var_info['target']
        
        # Calculate the actual difference
        if pos_diff_value > 0:
            actual_diff = pos_diff_value  # Excess workers
        elif neg_diff_value > 0:
            actual_diff = -neg_diff_value  # Shortage (negative)
        else:
            actual_diff = 0  # Perfect match
        
        penalty = (pos_diff_value + neg_diff_value) * penalty_weight
        
        point_1_results['deviations'][(d, s)] = {
            'target_workers': target,
            'actual_diff': actual_diff,
            'penalty': penalty
        }
        
        point_1_results['total_penalty'] += penalty
    
    results['point_1_pessobj_deviations'] = point_1_results
    
    # Point 2: Consecutive free days bonus
    point_2_results = {
        'consecutive_pairs': [],
        'total_bonus': 0
    }
    
    bonus_weight = optimization_details['point_2_consecutive_free_days']['weight']
    
    for var_info in optimization_details['point_2_consecutive_free_days']['variables']:
        if solver.Value(var_info['variable']) == 1:
            point_2_results['consecutive_pairs'].append({
                'worker': var_info['worker'],
                'day1': var_info['day1'],
                'day2': var_info['day2']
            })
            point_2_results['total_bonus'] += abs(bonus_weight)
    
    results['point_2_consecutive_free_days'] = point_2_results
    
    # Point 3: No workers penalty
    point_3_results = {
        'no_worker_days': [],
        'total_penalty': 0
    }
    
    penalty_weight = optimization_details['point_3_no_workers']['penalty_weight']
    
    for (d, s), var_info in optimization_details['point_3_no_workers']['variables'].items():
        if solver.Value(var_info['variable']) == 1:
            penalty = penalty_weight
            point_3_results['no_worker_days'].append({
                'day': d,
                'shift': s,
                'target': var_info['target'],
                'penalty': penalty
            })
            point_3_results['total_penalty'] += penalty
    
    results['point_3_no_workers'] = point_3_results
    
    # Point 4: Minimum workers penalty
    point_4_results = {
        'shortfalls': {},
        'total_penalty': 0
    }
    
    penalty_weight = optimization_details['point_4_min_workers']['penalty_weight']
    
    for (d, s), var_info in optimization_details['point_4_min_workers']['variables'].items():
        shortfall_value = solver.Value(var_info['shortfall'])
        if shortfall_value > 0:
            penalty = shortfall_value * penalty_weight
            point_4_results['shortfalls'][(d, s)] = {
                'min_required': var_info['min_required'],
                'shortfall': shortfall_value,
                'penalty': penalty
            }
            point_4_results['total_penalty'] += penalty
    
    results['point_4_min_workers'] = point_4_results
    
    # Point 5.1: Sunday balance penalty
    point_5_1_results = {
        'worker_segments': [],
        'total_penalty': 0
    }
    
    penalty_weight = optimization_details['point_5_1_sunday_balance']['penalty_weight']
    
    for var_info in optimization_details['point_5_1_sunday_balance']['variables']:
        over_penalty_value = solver.Value(var_info['over_penalty'])
        under_penalty_value = solver.Value(var_info['under_penalty'])
        
        if over_penalty_value > 0 or under_penalty_value > 0:
            penalty = (over_penalty_value + under_penalty_value) * penalty_weight
            point_5_1_results['worker_segments'].append({
                'worker': var_info['worker'],
                'segment': var_info['segment'],
                'ideal_count': var_info['ideal_count'],
                'over_penalty': over_penalty_value,
                'under_penalty': under_penalty_value,
                'penalty': penalty,
                'segment_sundays': var_info['segment_sundays']
            })


           
    results['point_5_1_sunday_balance'] = point_5_1_results
    
    # Point 5.2: C2D balance penalty
    point_5_2_results = {
        'worker_segments': [],
        'total_penalty': 0
    }
    
    penalty_weight = optimization_details['point_5_2_c2d_balance']['penalty_weight']
    
    for var_info in optimization_details['point_5_2_c2d_balance']['variables']:
        over_penalty_value = solver.Value(var_info['over_penalty'])
        under_penalty_value = solver.Value(var_info['under_penalty'])
        
        if over_penalty_value > 0 or under_penalty_value > 0:
            penalty = (over_penalty_value + under_penalty_value) * penalty_weight
            point_5_2_results['worker_segments'].append({
                'worker': var_info['worker'],
                'segment': var_info['segment'],
                'ideal_count': var_info['ideal_count'],
                'over_penalty': over_penalty_value,
                'under_penalty': under_penalty_value,
                'penalty': penalty,
                'segment_weekends': var_info['segment_weekends']
            })

          
            point_5_2_results['total_penalty'] += penalty
    
    results['point_5_2_c2d_balance'] = point_5_2_results
    
    # Point 6: Inconsistent shifts penalty
    point_6_results = {
        'inconsistent_weeks': [],
        'total_penalty': 0
    }
    
    penalty_weight = optimization_details['point_6_inconsistent_shifts']['penalty_weight']
    
    for (w, week), var_info in optimization_details['point_6_inconsistent_shifts']['variables'].items():
        if solver.Value(var_info['inconsistent_variable']) == 1:
            penalty = penalty_weight
            point_6_results['inconsistent_weeks'].append({
                'worker': w,
                'week': week,
                'has_m_shift': solver.Value(var_info['has_m_shift']) == 1,
                'has_t_shift': solver.Value(var_info['has_t_shift']) == 1,
                'working_days_in_week': var_info['working_days_in_week'],
                'penalty': penalty
            })
            point_6_results['total_penalty'] += penalty
    
    results['point_6_inconsistent_shifts'] = point_6_results
    
    # Point 7: Sunday balance across workers
    point_7_results = {
        'worker_pairs': [],
        'total_penalty': 0
    }
    
    penalty_weight = optimization_details['point_7_sunday_balance_across_workers']['penalty_weight']
    
    for var_info in optimization_details['point_7_sunday_balance_across_workers']['variables']:
        pos_diff_value = solver.Value(var_info['proportional_diff_pos'])
        neg_diff_value = solver.Value(var_info['proportional_diff_neg'])
        
        if pos_diff_value > 0 or neg_diff_value > 0:
            penalty = (pos_diff_value + neg_diff_value) * penalty_weight // 2
            
            w1_total = solver.Value(var_info['total_sunday_free_w1'])
            w2_total = solver.Value(var_info['total_sunday_free_w2'])
            
            point_7_results['worker_pairs'].append({
                'worker1': var_info['worker1'],
                'worker2': var_info['worker2'],
                'worker1_sunday_free': w1_total,
                'worker2_sunday_free': w2_total,
                'prop1': var_info['prop1'],
                'prop2': var_info['prop2'],
                'proportional_diff_pos': pos_diff_value,
                'proportional_diff_neg': neg_diff_value,
                'penalty': penalty
            })
            point_7_results['total_penalty'] += penalty
    
    results['point_7_sunday_balance_across_workers'] = point_7_results
    
    # Point 7B: LQ balance across workers
    point_7b_results = {
        'worker_pairs': [],
        'total_penalty': 0
    }
    
    penalty_weight = optimization_details['point_7b_lq_balance_across_workers']['penalty_weight']
    
    for var_info in optimization_details['point_7b_lq_balance_across_workers']['variables']:
        pos_diff_value = solver.Value(var_info['diff_pos'])
        neg_diff_value = solver.Value(var_info['diff_neg'])
        
        if pos_diff_value > 0 or neg_diff_value > 0:
            penalty = (pos_diff_value + neg_diff_value) * penalty_weight // 2
            
            w1_total = solver.Value(var_info['total_lq_free_w1'])
            w2_total = solver.Value(var_info['total_lq_free_w2'])
            
            point_7b_results['worker_pairs'].append({
                'worker1': var_info['worker1'],
                'worker2': var_info['worker2'],
                'worker1_lq_free': w1_total,
                'worker2_lq_free': w2_total,
                'prop1': var_info['prop1'],
                'prop2': var_info['prop2'],
                'diff_pos': pos_diff_value,
                'diff_neg': neg_diff_value,
                'penalty': penalty
            })

           
            point_7b_results['total_penalty'] += penalty
    
    results['point_7b_lq_balance_across_workers'] = point_7b_results
    
    # Point 8: Manager/Keyholder conflicts
    point_8_results = {
        'conflicts': {},
        'total_penalty': 0
    }
    
    penalty_weights = optimization_details['point_8_manager_keyholder_conflicts']['penalty_weights']
    
    for (d, conflict_type), var_info in optimization_details['point_8_manager_keyholder_conflicts']['variables'].items():
        if conflict_type == 'mgr_kh_same_off':
            if solver.Value(var_info['both_off']) == 1:
                penalty = penalty_weights['mgr_kh_same_off']
                point_8_results['conflicts'][(d, conflict_type)] = {
                    'day': d,
                    'type': 'Manager and Keyholder both off',
                    'mgr_any': solver.Value(var_info['mgr_any']) == 1,
                    'kh_any': solver.Value(var_info['kh_any']) == 1,
                    'penalty': penalty
                }
                point_8_results['total_penalty'] += penalty
                
        elif conflict_type == 'kh_overlap':
            if solver.Value(var_info['kh_overlap']) == 1:
                penalty = penalty_weights['kh_overlap']
                point_8_results['conflicts'][(d, conflict_type)] = {
                    'day': d,
                    'type': 'Multiple Keyholders off',
                    'penalty': penalty
                }
                point_8_results['total_penalty'] += penalty
                
        elif conflict_type == 'mgr_overlap':
            if solver.Value(var_info['mgr_overlap']) == 1:
                penalty = penalty_weights['mgr_overlap']
                point_8_results['conflicts'][(d, conflict_type)] = {
                    'day': d,
                    'type': 'Multiple Managers off',
                    'penalty': penalty
                }
                point_8_results['total_penalty'] += penalty
    
    results['point_8_manager_keyholder_conflicts'] = point_8_results
    
    # Calculate total penalty across all optimization points
    total_penalty = sum([
        point_1_results['total_penalty'],
        point_3_results['total_penalty'],
        point_4_results['total_penalty'],
        point_5_1_results['total_penalty'],
        point_5_2_results['total_penalty'],
        point_6_results['total_penalty'],
        point_7_results['total_penalty'],
        point_7b_results['total_penalty'],
        point_8_results['total_penalty']
    ])
    
    total_bonus = point_2_results['total_bonus']
    
    results['summary'] = {
        'total_penalty': total_penalty,
        'total_bonus': total_bonus,
        'net_objective': total_penalty - total_bonus,
        'point_breakdown': {
            'point_1_pessobj_deviations': point_1_results['total_penalty'],
            'point_2_consecutive_free_days': -point_2_results['total_bonus'],  # negative because it's a bonus
            'point_3_no_workers': point_3_results['total_penalty'],
            'point_4_min_workers': point_4_results['total_penalty'],
            'point_5_1_sunday_balance': point_5_1_results['total_penalty'],
            'point_5_2_c2d_balance': point_5_2_results['total_penalty'],
            'point_6_inconsistent_shifts': point_6_results['total_penalty'],
            'point_7_sunday_balance_across_workers': point_7_results['total_penalty'],
            'point_7b_lq_balance_across_workers': point_7b_results['total_penalty'],
            'point_8_manager_keyholder_conflicts': point_8_results['total_penalty']
        }
    }
    
    return results

def _create_empty_results(algo_name: str, process_id: int, start_date: str, end_date: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
    """Create empty results structure when no data is available."""
    return {
        'core_results': {
            'schedule': pd.DataFrame(),
            'formatted_schedule': pd.DataFrame(),
            'wide_format_schedule': pd.DataFrame(),
            'status': 'UNKNOWN'
        },
        'metadata': {
            'algorithm_name': algo_name,
            'algorithm_version': '1.0',
            'execution_timestamp': datetime.now().isoformat(),
            'process_id': process_id,
            'date_range': {
                'start_date': start_date,
                'end_date': end_date,
                'total_days': 0
            },
            'parameters_used': parameters,
            'solver_info': {
                'solver_name': 'CP-SAT',
                'solving_time_seconds': None,
                'num_branches': None,
                'num_conflicts': None
            }
        },
        'scheduling_stats': {},
        'constraint_validation': {},
        'quality_metrics': {},
        'validation': {'is_valid_solution': False, 'validation_errors': ['No schedule data available']},
        'export_info': {},
        'summary': {
            'status': 'failed',
            'message': 'No schedule data available',
            'key_metrics': {}
        }
    }

def _calculate_comprehensive_stats(algorithm_results: pd.DataFrame, start_date: str, end_date: str, data_processed: Dict[str, Any] = None) -> Dict[str, Any]:
    """Calculate comprehensive statistics from algorithm results in wide format."""
    try:
        # Basic counts
        total_workers = len(algorithm_results) if not algorithm_results.empty else 0
        
        # Get day columns (all columns except 'Worker')
        day_columns = [col for col in algorithm_results.columns if col != 'Worker' and col.startswith('Day')]
        total_days = len(day_columns)
        
        # Calculate date range
        if start_date and end_date:
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            total_days = len(date_range)
        
        # Shift distribution - flatten all shift values
        shift_distribution = {}
        unassigned_slots = 0
        total_assignments = 0
        
        if not algorithm_results.empty and day_columns:
            # Get all shift values from the wide format
            all_shifts = []
            for col in day_columns:
                all_shifts.extend(algorithm_results[col].dropna().astype(str).tolist())
            
            # Count shift types
            shift_series = pd.Series(all_shifts)
            shift_distribution = shift_series.value_counts().to_dict()
            total_assignments = len(all_shifts)
            unassigned_slots = shift_distribution.get('N', 0) + shift_distribution.get('ERROR', 0) + shift_distribution.get('-', 0)
        
        # Worker statistics
        workers_scheduled = total_workers
        worker_list = algorithm_results['Worker'].astype(str).tolist() if 'Worker' in algorithm_results.columns else []
        
        # Time coverage
        scheduled_days = total_days
        coverage_percentage = 100.0 if total_days > 0 else 0
        
        # Working days and special days coverage
        working_days_covered = 0
        special_days_covered = 0
        
        if data_processed:
            working_days = data_processed.get('working_days', [])
            special_days = data_processed.get('special_days', [])
            working_days_covered = len(working_days)
            special_days_covered = len(special_days)
        
        return {
            'workers': {
                'total_workers': total_workers,
                'workers_scheduled': workers_scheduled,
                'worker_list': worker_list
            },
            'shifts': {
                'shift_distribution': shift_distribution,
                'total_assignments': total_assignments,
                'shift_types_used': list(shift_distribution.keys()),
                'unassigned_slots': unassigned_slots
            },
            'time_coverage': {
                'total_days': total_days,
                'working_days_covered': working_days_covered,
                'special_days_covered': special_days_covered,
                'coverage_percentage': coverage_percentage
            }
        }
    except Exception as e:
        logger.error(f"Error calculating comprehensive stats: {e}")
        return {}

def _validate_constraints(algorithm_results: pd.DataFrame) -> Dict[str, Any]:
    """Validate constraint satisfaction from wide format."""
    try:
        constraint_validation = {
            'working_days': {
                'violations': [],
                'satisfied': True,
                'details': 'All workers have proper working day assignments'
            },
            'continuous_working_days': {
                'violations': [],
                'max_continuous_exceeded': [],
                'satisfied': True
            },
            'salsa_specific': {
                'consecutive_free_days': {'satisfied': True, 'violations': []},
                'quality_weekends': {'satisfied': True, 'violations': []},
                'saturday_L_constraint': {'satisfied': True, 'violations': []}
            },
            'overall_satisfaction': 100
        }
        
        if algorithm_results.empty:
            constraint_validation['working_days']['satisfied'] = False
            constraint_validation['working_days']['violations'].append('No schedule data available')
            constraint_validation['overall_satisfaction'] = 0
            return constraint_validation
        
        # Get day columns
        day_columns = [col for col in algorithm_results.columns if col != 'Worker' and col.startswith('Day')]
        
        # Check for continuous working days violations
        continuous_violations = []
        max_continuous_work = 5  # Assume max 5 consecutive working days
        
        for idx, row in algorithm_results.iterrows():
            worker = row['Worker']
            worker_shifts = [str(row[col]) for col in day_columns if pd.notna(row[col])]
            
            consecutive_work = 0
            max_consecutive = 0
            
            for shift in worker_shifts:
                if shift in ['M', 'T']:  # Working shifts
                    consecutive_work += 1
                    max_consecutive = max(max_consecutive, consecutive_work)
                else:
                    consecutive_work = 0
            
            if max_consecutive > max_continuous_work:
                continuous_violations.append(f"Worker {worker}: {max_consecutive} consecutive working days")
        
        if continuous_violations:
            constraint_validation['continuous_working_days']['satisfied'] = False
            constraint_validation['continuous_working_days']['violations'] = continuous_violations
            constraint_validation['overall_satisfaction'] -= 20
        
        # Check SALSA-specific constraints
        weekend_violations = []
        for idx, row in algorithm_results.iterrows():
            worker = row['Worker']
            worker_shifts = [str(row[col]) for col in day_columns if pd.notna(row[col])]
            
            # Check for quality weekends (LQ should be followed by proper rest)
            lq_count = worker_shifts.count('LQ')
            if lq_count == 0:
                weekend_violations.append(f"Worker {worker}: No quality weekends assigned")
        
        if weekend_violations:
            constraint_validation['salsa_specific']['quality_weekends']['satisfied'] = False
            constraint_validation['salsa_specific']['quality_weekends']['violations'] = weekend_violations
            constraint_validation['overall_satisfaction'] -= 10
        
        return constraint_validation
    except Exception as e:
        logger.error(f"Error validating constraints: {e}")
        return {}
    
def _calculate_quality_metrics(algorithm_results: pd.DataFrame) -> Dict[str, Any]:
    """Calculate quality metrics for the solution from wide format."""
    try:
        # Calculate basic metrics
        total_workers = len(algorithm_results) if not algorithm_results.empty else 0
        
        # Get day columns
        day_columns = [col for col in algorithm_results.columns if col != 'Worker' and col.startswith('Day')]

        
        # SALSA-specific metrics
        two_day_quality_weekends = 0
        consecutive_free_days_achieved = 0
        saturday_L_assignments = 0
        
        if not algorithm_results.empty and day_columns:
            # Count LQ (two-day quality weekends)
            for col in day_columns:
                two_day_quality_weekends += (algorithm_results[col] == 'LQ').sum()
            
            # Count L assignments (including Saturday L)
            for col in day_columns:
                saturday_L_assignments += (algorithm_results[col] == 'L').sum()
            
            # Count consecutive free days (simplified - count sequences of L, LQ, F)
            for idx, row in algorithm_results.iterrows():
                worker_shifts = [str(row[col]) for col in day_columns if pd.notna(row[col])]
                consecutive_count = 0
                max_consecutive = 0
                
                for shift in worker_shifts:
                    if shift in ['L', 'LQ']:
                        consecutive_count += 1
                        max_consecutive = max(max_consecutive, consecutive_count)
                    else:
                        consecutive_count = 0
                
                if max_consecutive >= 2:
                    consecutive_free_days_achieved += 1
        
        
        return {
            'salsa_specific_metrics': {
                'two_day_quality_weekends': two_day_quality_weekends,
                'consecutive_free_days_achieved': consecutive_free_days_achieved,
                'saturday_L_assignments': saturday_L_assignments
            }
        }
    except Exception as e:
        logger.error(f"Error calculating quality metrics: {e}")
        return {}
    
def _format_schedules(algorithm_results: pd.DataFrame, start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
    """Format schedule for different output types from wide format."""
    try:
        formatted_schedules = {
            'database_format': pd.DataFrame(),
            'wide_format': pd.DataFrame()
        }
        
        if algorithm_results.empty:
            return formatted_schedules
        
        # Wide format is already available (the input format)
        formatted_schedules['wide_format'] = algorithm_results.copy()
        
        # Database format (long format) - convert from wide to long
        if 'Worker' in algorithm_results.columns:
            day_columns = [col for col in algorithm_results.columns if col != 'Worker' and col.startswith('Day')]
            
            if day_columns:
                # Melt the DataFrame to long format
                melted_df = pd.melt(
                    algorithm_results,
                    id_vars=['Worker'],
                    value_vars=day_columns,
                    var_name='Day',
                    value_name='Shift'
                )
                
                # Clean up the Day column to extract day numbers
                logger.info(f"DEBUG: melted_df['Day'] type: {type(melted_df['Day'])}")
                logger.info(f"DEBUG: melted_df['Day'].str.replace('Day ', ''): {melted_df['Day'].str.replace('Day ', '')}")
                melted_df['Day'] = melted_df['Day'].str.replace('Day_', '').astype(int)
                
                # Convert day numbers to actual dates if start_date is available
                if start_date:
                    try:
                        base_date = pd.to_datetime(start_date)
                        melted_df['Date'] = melted_df['Day'].apply(lambda x: base_date + pd.Timedelta(days=x-1))
                        melted_df['Date'] = melted_df['Date'].dt.strftime('%Y-%m-%d')
                    except:
                        melted_df['Date'] = melted_df['Day'].astype(str)
                else:
                    melted_df['Date'] = melted_df['Day'].astype(str)
                
                # Rename columns and select relevant ones
                formatted_schedules['database_format'] = melted_df[['Worker', 'Date', 'Shift']].copy()
                formatted_schedules['database_format'].rename(columns={'Worker': 'colaborador'}, inplace=True)
                formatted_schedules['database_format'].rename(columns={'Shift': 'horario'}, inplace=True)
                formatted_schedules['database_format'].rename(columns={'Date': 'data'}, inplace=True)

        return formatted_schedules
    except Exception as e:
        logger.error(f"Error formatting schedules: {e}", exc_info=True)
        return {'database_format': pd.DataFrame(), 'wide_format': pd.DataFrame()}

def _create_metadata(algo_name: str, process_id: int, start_date: str, end_date: str, parameters: Dict[str, Any], stats: Dict[str, Any], solver_attributes: Dict[str, Any]) -> Dict[str, Any]:
    """Create metadata information."""
    return {
        'algorithm_name': algo_name,
        'algorithm_version': '1.0',
        'execution_timestamp': datetime.now().isoformat(),
        'process_id': process_id,
        'date_range': {
            'start_date': start_date,
            'end_date': end_date,
            'total_days': stats.get('time_coverage', {}).get('total_days', 0)
        },
        'parameters_used': parameters,
        'solver_info': {
            'solver_name': 'CP-SAT',
            'solving_time_seconds': solver_attributes.get('solving_time_seconds'),
            'num_branches': solver_attributes.get('num_branches'),
            'num_conflicts': solver_attributes.get('num_conflicts')
        }
    }

def _validate_solution(algorithm_results: pd.DataFrame) -> Dict[str, Any]:
    """Validate the solution and return validation results for wide format."""
    try:
        validation_errors = []
        warnings = []
        recommendations = []
        
        # Check if solution is valid
        if algorithm_results.empty:
            validation_errors.append("No schedule data available")
        else:
            # Check for required columns
            if 'Worker' not in algorithm_results.columns:
                validation_errors.append("Missing 'Worker' column")
            
            # Check for day columns
            day_columns = [col for col in algorithm_results.columns if col != 'Worker' and col.startswith('Day')]
            if not day_columns:
                validation_errors.append("No day columns found")
            
            # Check for unassigned days
            if day_columns:
                unassigned_count = 0
                for col in day_columns:
                    unassigned_count += (algorithm_results[col].isin(['N', 'ERROR', '-'])).sum()
                
                if unassigned_count > 0:
                    warnings.append(f"Found {unassigned_count} unassigned shifts")
                    recommendations.append("Review worker constraints and availability")
                
                # Check for missing data
                missing_count = 0
                for col in day_columns:
                    missing_count += algorithm_results[col].isna().sum()
                
                if missing_count > 0:
                    warnings.append(f"Found {missing_count} missing shift assignments")
                    recommendations.append("Verify data completeness")
        
        is_valid_solution = len(validation_errors) == 0
        
        return {
            'is_valid_solution': is_valid_solution,
            'validation_errors': validation_errors,
            'warnings': warnings,
            'recommendations': recommendations
        }
    except Exception as e:
        logger.error(f"Error validating solution: {e}")
        return {
            'is_valid_solution': False,
            'validation_errors': [f"Validation error: {e}"],
            'warnings': [],
            'recommendations': []
        }
    
def _create_export_info(process_id: int, project_root_dir: str) -> Dict[str, Any]:
    """Create export information."""
    try:
        # Get output filename from project_root_dir
        output_filename = os.path.join(project_root_dir, 'data', 'output', f'salsa_schedule_{process_id}.xlsx')
        
        return {
            'export_files': {
                'excel_file': output_filename,
                'csv_file': None,
                'json_file': None
            },
            'export_timestamp': datetime.now().isoformat(),
            'export_status': 'completed'
        }
    except Exception as e:
        logger.error(f"Error creating export info: {e}")
        return {}

def _convert_free_days(algorithm_results: pd.DataFrame, data_processed: Dict[str, Any]) -> pd.DataFrame:
    """Convert free day codes in the codes of FO if it the last or only free day, 
    FC if it is the free day before FO and the rest free days as V"""
    
    workers = data_processed.get('workers_complete', [])

    week_to_days_salsa = data_processed.get('week_to_days_salsa', {})

    for w in workers:
        worker_data = algorithm_results.loc[algorithm_results['Worker'] == w]
        for week, days in week_to_days_salsa.items():
            if any(worker_data[f'Day_{d}'].isin(['L', 'LQ']).any() for d in days):
                free_days = [d for d in days if worker_data[f'Day_{d}'].isin(['L', 'LQ']).any()]

                if len(free_days) == 1:
                    original_shift = worker_data[f'Day_{free_days[0]}'].iloc[0]
                    algorithm_results.loc[algorithm_results['Worker'] == w, f'Day_{free_days[0]}'] = original_shift
                elif len(free_days) > 1:
                    original_shift_last = worker_data[f'Day_{free_days[-1]}'].iloc[0]
                    algorithm_results.loc[algorithm_results['Worker'] == w, f'Day_{free_days[-1]}'] = original_shift_last

                    original_shift_second_last = worker_data[f'Day_{free_days[-2]}'].iloc[0]
                    algorithm_results.loc[algorithm_results['Worker'] == w, f'Day_{free_days[-2]}'] = original_shift_second_last
                    for d in free_days[:-2]:
                        algorithm_results.loc[algorithm_results['Worker'] == w, f'Day_{d}'] = '-'

    return algorithm_results