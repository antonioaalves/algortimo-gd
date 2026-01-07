"""
Integration validation script for df_calendario generation.

This script validates that the different calendario "layers" are being applied correctly:
- section_employees_id_list → All employees in section
- ausencias_ferias → Vacation/absence markers
- ciclos_completos → Complete 90-day cycles
- folgas_ciclos → Cycle days off
- calendario_passado → Past calendar data
- days_off → Scheduled days off
- df_feriados → Holidays

Usage:
    python tests/integration/validate_calendario_integration.py --process-id 11504 --posto-id 613
    
Or run all validations:
    python tests/integration/validate_calendario_integration.py --all
"""

import pandas as pd
import argparse
import os
import sys
from typing import Tuple, List, Dict
from datetime import datetime

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'data', 'output')


def load_dataframes(process_id: int, posto_id: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load df_calendario and df_colaborador for a given process/posto."""
    calendario_path = os.path.join(OUTPUT_DIR, f'df_calendario-{process_id}-{posto_id}.csv')
    colaborador_path = os.path.join(OUTPUT_DIR, f'df_colaborador-{process_id}-{posto_id}.csv')
    
    if not os.path.exists(calendario_path):
        raise FileNotFoundError(f"df_calendario not found: {calendario_path}")
    if not os.path.exists(colaborador_path):
        raise FileNotFoundError(f"df_colaborador not found: {colaborador_path}")
    
    df_calendario = pd.read_csv(calendario_path)
    df_colaborador = pd.read_csv(colaborador_path)
    
    return df_calendario, df_colaborador


def validate_employee_consistency(df_calendario: pd.DataFrame, df_colaborador: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate that df_calendario has entries for all employees in df_colaborador.
    
    This is the KEY test for the section_employees_id_list fix:
    - In section mode: both should have same employees
    - In single-employee mode: df_calendario should have ALL section employees,
      df_colaborador might have fewer (only those being generated)
    """
    errors = []
    
    cal_employees = set(df_calendario['employee_id'].unique())
    col_employees = set(df_colaborador['employee_id'].unique())
    
    print(f"\n{'='*60}")
    print("VALIDATION 1: Employee Consistency")
    print(f"{'='*60}")
    print(f"  df_calendario employees: {sorted(cal_employees)}")
    print(f"  df_colaborador employees: {sorted(col_employees)}")
    
    # Check if colaborador employees are a subset of calendario employees
    missing_in_cal = col_employees - cal_employees
    if missing_in_cal:
        errors.append(f"Employees in df_colaborador but NOT in df_calendario: {missing_in_cal}")
    
    # Note: It's OK if calendario has MORE employees than colaborador (single-employee mode)
    extra_in_cal = cal_employees - col_employees
    if extra_in_cal:
        print(f"  INFO: df_calendario has additional employees (expected in single-employee mode): {extra_in_cal}")
    
    if not errors:
        print(f"   PASSED: All colaborador employees have calendario entries")
    else:
        for e in errors:
            print(f"  ✗ FAILED: {e}")
    
    return len(errors) == 0, errors


def validate_rows_per_employee(df_calendario: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate that all employees have the same number of rows.
    
    Each employee should have: num_days * num_shifts (usually 2: M and T) rows.
    """
    errors = []
    
    rows_per_emp = df_calendario.groupby('employee_id').size()
    unique_counts = rows_per_emp.unique()
    
    print(f"\n{'='*60}")
    print("VALIDATION 2: Rows per Employee")
    print(f"{'='*60}")
    print(f"  Rows per employee: {rows_per_emp.to_dict()}")
    
    if len(unique_counts) > 1:
        errors.append(f"Inconsistent row counts across employees: {rows_per_emp.to_dict()}")
        print(f"  ✗ FAILED: Different employees have different row counts")
    else:
        print(f"   PASSED: All employees have {unique_counts[0]} rows")
    
    return len(errors) == 0, errors


def validate_horario_distribution(df_calendario: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate the distribution of horario values.
    
    Expected values:
    - '-' : Unassigned (to be filled by algorithm)
    - 'M', 'T', 'MoT' : Shift assignments
    - 'L' : Days off
    - 'LQ' : Quality leave
    - 'F' : Closed holidays
    - 'AUS', 'FER' : Absences, vacations
    """
    errors = []
    
    horario_counts = df_calendario['horario'].value_counts()
    
    print(f"\n{'='*60}")
    print("VALIDATION 3: Horario Value Distribution")
    print(f"{'='*60}")
    print(f"  Horario counts:")
    for val, count in horario_counts.items():
        pct = count / len(df_calendario) * 100
        print(f"    {val:>6}: {count:>6} ({pct:.1f}%)")
    
    # Check for unexpected values
    expected_values = {'-', 'M', 'T', 'MoT', 'L', 'LQ', 'F', 'AUS', 'FER', 'CICLO', 'X'}
    actual_values = set(df_calendario['horario'].dropna().unique())
    unexpected = actual_values - expected_values
    
    if unexpected:
        print(f"  INFO: Found additional horario values (may be valid): {unexpected}")
    
    # Warn if everything is unassigned
    unassigned_pct = (df_calendario['horario'] == '-').sum() / len(df_calendario) * 100
    if unassigned_pct > 95:
        errors.append(f"Warning: {unassigned_pct:.1f}% of rows are unassigned (-). Calendario layers may not be applied.")
        print(f"  ⚠ WARNING: Very high unassigned percentage ({unassigned_pct:.1f}%)")
    else:
        print(f"   PASSED: Horario distribution looks reasonable")
    
    return len(errors) == 0, errors


def validate_dia_tipo_distribution(df_calendario: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate dia_tipo (day type) distribution.
    
    Expected values:
    - Weekday names: Mon, Tue, Wed, Thu, Fri, Sat
    - Sunday variants: domYf (open Sunday), domNf (closed Sunday)
    - Holiday variants: ferYf (open holiday), ferNf (closed holiday)
    """
    errors = []
    
    dia_tipo_counts = df_calendario['dia_tipo'].value_counts()
    
    print(f"\n{'='*60}")
    print("VALIDATION 4: Dia Tipo Distribution")
    print(f"{'='*60}")
    print(f"  Dia tipo counts:")
    for val, count in dia_tipo_counts.items():
        print(f"    {val:>8}: {count:>6}")
    
    # Check for Sundays and holidays
    has_sundays = any('dom' in str(v).lower() for v in dia_tipo_counts.index)
    has_holidays = any('fer' in str(v).lower() for v in dia_tipo_counts.index)
    
    if has_sundays:
        print(f"   Sunday markers present")
    else:
        print(f"  ⚠ No Sunday markers found (domYf/domNf)")
    
    if has_holidays:
        print(f"   Holiday markers present")
    else:
        print(f"  INFO: No holiday markers found (ferYf/ferNf) - may be expected if no holidays in range")
    
    return True, errors  # This is informational, not a hard failure


def validate_calendario_layers_applied(df_calendario: pd.DataFrame, df_colaborador: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate that calendario "layers" are being applied by checking for non-'-' horario values.
    
    Layers:
    - ausencias_ferias: Should show AUS, FER, or similar
    - ciclos_completos: For employees with ciclo='Completo', should have M/T patterns
    - folgas_ciclos: Should have L markers on specific dates
    - calendario_passado: Past data should have M/T/L patterns
    """
    errors = []
    
    print(f"\n{'='*60}")
    print("VALIDATION 5: Calendario Layers Applied")
    print(f"{'='*60}")
    
    # Check each employee for non-empty horario values
    for emp_id in df_calendario['employee_id'].unique():
        emp_data = df_calendario[df_calendario['employee_id'] == emp_id]
        non_empty = emp_data[emp_data['horario'] != '-']
        
        if len(non_empty) > 0:
            # Get breakdown of non-empty values
            breakdown = non_empty['horario'].value_counts().to_dict()
            print(f"  Employee {emp_id}: {len(non_empty)} pre-filled entries {breakdown}")
        else:
            print(f"  Employee {emp_id}: No pre-filled entries (all '-')")
    
    # Check if employees marked as 'Completo' have ciclo entries
    if 'ciclo' in df_colaborador.columns:
        ciclo_employees = df_colaborador[df_colaborador['ciclo'] == 'Completo']['employee_id'].tolist()
        if ciclo_employees:
            print(f"\n  Employees with ciclo='Completo': {ciclo_employees}")
            for emp_id in ciclo_employees:
                emp_cal = df_calendario[df_calendario['employee_id'] == emp_id]
                has_shifts = emp_cal['horario'].isin(['M', 'T', 'MoT']).any()
                if has_shifts:
                    print(f"     Employee {emp_id} has shift markers from ciclo")
                else:
                    print(f"    ⚠ Employee {emp_id} has ciclo='Completo' but no shift markers")
    
    print(f"\n   Layer validation complete (review above for details)")
    return True, errors


def validate_date_range(df_calendario: pd.DataFrame) -> Tuple[bool, List[str]]:
    """Validate the date range in df_calendario."""
    errors = []
    
    df_calendario['schedule_day'] = pd.to_datetime(df_calendario['schedule_day'])
    min_date = df_calendario['schedule_day'].min()
    max_date = df_calendario['schedule_day'].max()
    unique_dates = df_calendario['schedule_day'].nunique()
    
    print(f"\n{'='*60}")
    print("VALIDATION 6: Date Range")
    print(f"{'='*60}")
    print(f"  Date range: {min_date.date()} to {max_date.date()}")
    print(f"  Unique dates: {unique_dates}")
    print(f"   Date range validation complete")
    
    return True, errors


def run_all_validations(process_id: int, posto_id: int) -> bool:
    """Run all validations for a given process/posto."""
    print(f"\n{'#'*60}")
    print(f"# CALENDARIO INTEGRATION VALIDATION")
    print(f"# Process ID: {process_id}, Posto ID: {posto_id}")
    print(f"# Timestamp: {datetime.now().isoformat()}")
    print(f"{'#'*60}")
    
    try:
        df_calendario, df_colaborador = load_dataframes(process_id, posto_id)
        print(f"\nLoaded df_calendario: {len(df_calendario)} rows")
        print(f"Loaded df_colaborador: {len(df_colaborador)} rows")
    except FileNotFoundError as e:
        print(f"\n✗ ERROR: {e}")
        return False
    
    all_passed = True
    
    # Run validations
    validations = [
        ("Employee Consistency", validate_employee_consistency),
        ("Rows per Employee", validate_rows_per_employee),
        ("Horario Distribution", validate_horario_distribution),
        ("Dia Tipo Distribution", validate_dia_tipo_distribution),
        ("Calendario Layers", validate_calendario_layers_applied),
        ("Date Range", validate_date_range),
    ]
    
    results = {}
    for name, validation_func in validations:
        if validation_func == validate_calendario_layers_applied:
            passed, errors = validation_func(df_calendario, df_colaborador)
        else:
            passed, errors = validation_func(df_calendario) if validation_func != validate_employee_consistency else validation_func(df_calendario, df_colaborador)
        results[name] = (passed, errors)
        if not passed:
            all_passed = False
    
    # Summary
    print(f"\n{'='*60}")
    print("VALIDATION SUMMARY")
    print(f"{'='*60}")
    for name, (passed, errors) in results.items():
        status = " PASSED" if passed else "✗ FAILED"
        print(f"  {status}: {name}")
    
    if all_passed:
        print(f"\n ALL VALIDATIONS PASSED")
    else:
        print(f"\n✗ SOME VALIDATIONS FAILED - Review above for details")
    
    return all_passed


def find_available_outputs() -> List[Tuple[int, int]]:
    """Find all available output files."""
    outputs = []
    for filename in os.listdir(OUTPUT_DIR):
        if filename.startswith('df_calendario-') and filename.endswith('.csv'):
            parts = filename.replace('df_calendario-', '').replace('.csv', '').split('-')
            if len(parts) == 2:
                try:
                    process_id = int(parts[0])
                    posto_id = int(parts[1])
                    outputs.append((process_id, posto_id))
                except ValueError:
                    pass
    return outputs


def main():
    parser = argparse.ArgumentParser(description='Validate df_calendario integration')
    parser.add_argument('--process-id', type=int, help='Process ID')
    parser.add_argument('--posto-id', type=int, help='Posto ID')
    parser.add_argument('--all', action='store_true', help='Run validations on all available outputs')
    parser.add_argument('--list', action='store_true', help='List available outputs')
    
    args = parser.parse_args()
    
    if args.list:
        outputs = find_available_outputs()
        print("Available outputs:")
        for pid, posto in outputs:
            print(f"  Process {pid}, Posto {posto}")
        return
    
    if args.all:
        outputs = find_available_outputs()
        if not outputs:
            print("No output files found")
            return
        
        all_passed = True
        for process_id, posto_id in outputs:
            if not run_all_validations(process_id, posto_id):
                all_passed = False
        
        print(f"\n{'#'*60}")
        if all_passed:
            print("# ALL PROCESSES PASSED VALIDATION")
        else:
            print("# SOME PROCESSES FAILED VALIDATION")
        print(f"{'#'*60}")
    elif args.process_id and args.posto_id:
        run_all_validations(args.process_id, args.posto_id)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()

