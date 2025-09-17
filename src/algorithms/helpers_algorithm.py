import pandas as pd
import os
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

# Local stuff
from src.config import PROJECT_NAME
from base_data_project.log_config import get_logger

logger = get_logger(PROJECT_NAME)


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
    
def _create_export_info(process_id: int, ROOT_DIR) -> Dict[str, Any]:
    """Create export information."""
    try:
        # Get output filename from ROOT_DIR
        output_filename = os.path.join(ROOT_DIR, 'data', 'output', f'salsa_schedule_{process_id}.xlsx')
        
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
                    algorithm_results.loc[algorithm_results['Worker'] == w, f'Day_{free_days[0]}'] = 'FO'
                elif len(free_days) > 1:
                    algorithm_results.loc[algorithm_results['Worker'] == w, f'Day_{free_days[-1]}'] = 'FO'
                    algorithm_results.loc[algorithm_results['Worker'] == w, f'Day_{free_days[-2]}'] = 'FC'
                    for d in free_days[:-2]:
                        algorithm_results.loc[algorithm_results['Worker'] == w, f'Day_{d}'] = '-'

    return algorithm_results