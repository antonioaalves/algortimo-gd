#!/usr/bin/env python3
"""
CSV Process Comparison Tool
Compares CSV files from different processes (R vs Python) to validate identical results.
"""

import pandas as pd
import numpy as np
import os
import glob
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')


class CSVProcessComparator:
    def __init__(self, csv_directory: str = ".", tolerance: float = 1e-10, 
                 python_folder: str = None, r_folder: str = None):
        """
        Initialize the CSV comparator.
        
        Args:
            csv_directory: Directory containing CSV files (if not using separate folders)
            tolerance: Numerical tolerance for floating point comparisons
            python_folder: Folder containing Python-generated CSV files
            r_folder: Folder containing R-generated CSV files
        """
        self.csv_directory = Path(csv_directory)
        self.python_folder = Path(python_folder) if python_folder else None
        self.r_folder = Path(r_folder) if r_folder else None
        self.tolerance = tolerance
        self.csv_types = ['df_calendario', 'df_colaborador', 'df_estimativas']
        
    def discover_csv_files(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Discover all CSV files and group them by process_id and type.
        
        Returns:
            Dictionary structure: {process_id: {csv_type: [file_paths]}}
        """
        files_by_process = {}
        
        # Pattern to match: type-process_id.csv
        pattern = r'(df_calendario|df_colaborador|df_estimativas)-(\d+)\.csv'
        
        # Determine which directories to search
        search_dirs = []
        if self.python_folder and self.r_folder:
            search_dirs = [self.python_folder, self.r_folder]
        else:
            search_dirs = [self.csv_directory]
        
        # Find all CSV files in directories
        for directory in search_dirs:
            csv_files = list(directory.glob('*.csv'))
            
            for file_path in csv_files:
                match = re.match(pattern, file_path.name)
                if match:
                    csv_type, process_id = match.groups()
                    
                    if process_id not in files_by_process:
                        files_by_process[process_id] = {}
                    if csv_type not in files_by_process[process_id]:
                        files_by_process[process_id][csv_type] = []
                        
                    files_by_process[process_id][csv_type].append(str(file_path))
        
        return files_by_process
    
    def normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize dataframe to handle R vs Python differences.
        
        Args:
            df: Input dataframe
            
        Returns:
            Normalized dataframe
        """
        df_norm = df.copy()
        
        # Normalize column names (case-insensitive)
        df_norm.columns = df_norm.columns.str.lower().str.strip()
        
        # Handle different null representations
        df_norm = df_norm.replace(['NULL', 'null', 'NA', 'N/A', ''], np.nan)
        
        # Normalize string columns
        for col in df_norm.select_dtypes(include=['object']):
            if df_norm[col].dtype == 'object':
                # Strip whitespace and handle case sensitivity for non-numeric strings
                df_norm[col] = df_norm[col].astype(str).str.strip()
                # Try to convert to numeric if possible (handles R/Python numeric formatting)
                numeric_series = pd.to_numeric(df_norm[col], errors='ignore')
                if not numeric_series.equals(df_norm[col]):
                    df_norm[col] = numeric_series
        
        # Sort columns for consistent comparison
        df_norm = df_norm.reindex(sorted(df_norm.columns), axis=1)
        
        return df_norm
    
    def compare_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                          file1_name: str, file2_name: str) -> Dict:
        """
        Compare two dataframes and return detailed results.
        
        Args:
            df1, df2: Dataframes to compare
            file1_name, file2_name: File names for reporting
            
        Returns:
            Dictionary with comparison results
        """
        result = {
            'files_compared': (file1_name, file2_name),
            'identical': False,
            'shape_match': False,
            'columns_match': False,
            'differences': [],
            'summary': {}
        }
        
        # Normalize both dataframes
        df1_norm = self.normalize_dataframe(df1)
        df2_norm = self.normalize_dataframe(df2)
        
        # Check shapes
        result['summary']['df1_shape'] = df1.shape
        result['summary']['df2_shape'] = df2.shape
        result['shape_match'] = df1_norm.shape == df2_norm.shape
        
        if not result['shape_match']:
            result['differences'].append({
                'type': 'shape_mismatch',
                'details': f"Shape mismatch: {df1_norm.shape} vs {df2_norm.shape}"
            })
            return result
        
        # Check columns
        cols1_set = set(df1_norm.columns)
        cols2_set = set(df2_norm.columns)
        result['columns_match'] = cols1_set == cols2_set
        
        if not result['columns_match']:
            missing_in_df2 = cols1_set - cols2_set
            missing_in_df1 = cols2_set - cols1_set
            result['differences'].append({
                'type': 'column_mismatch',
                'details': {
                    'missing_in_df2': list(missing_in_df2),
                    'missing_in_df1': list(missing_in_df1)
                }
            })
            return result
        
        # Sort both dataframes by all columns for order-independent comparison
        try:
            # Create a composite sort key from all columns
            sort_columns = []
            for col in df1_norm.columns:
                if df1_norm[col].dtype in ['object', 'string']:
                    sort_columns.append(col)
                elif pd.api.types.is_numeric_dtype(df1_norm[col]):
                    sort_columns.append(col)
            
            if sort_columns:
                df1_sorted = df1_norm.sort_values(sort_columns).reset_index(drop=True)
                df2_sorted = df2_norm.sort_values(sort_columns).reset_index(drop=True)
            else:
                df1_sorted = df1_norm.reset_index(drop=True)
                df2_sorted = df2_norm.reset_index(drop=True)
        except Exception as e:
            # If sorting fails, compare without sorting
            df1_sorted = df1_norm.reset_index(drop=True)
            df2_sorted = df2_norm.reset_index(drop=True)
            result['differences'].append({
                'type': 'sorting_warning',
                'details': f"Could not sort for comparison: {str(e)}"
            })
        
        # Compare values column by column
        value_differences = []
        
        for col in df1_sorted.columns:
            col_diff = self.compare_column(df1_sorted[col], df2_sorted[col], col)
            if col_diff:
                value_differences.extend(col_diff)
        
        if value_differences:
            result['differences'].append({
                'type': 'value_differences',
                'details': value_differences[:50]  # Limit to first 50 differences
            })
        
        # Overall result
        result['identical'] = (result['shape_match'] and 
                              result['columns_match'] and 
                              len(value_differences) == 0)
        
        result['summary']['total_differences'] = len(value_differences)
        
        return result
    
    def compare_column(self, col1: pd.Series, col2: pd.Series, col_name: str) -> List[Dict]:
        """
        Compare two columns and return differences.
        
        Args:
            col1, col2: Series to compare
            col_name: Column name for reporting
            
        Returns:
            List of differences found
        """
        differences = []
        
        # Handle NaN values
        nan_mask1 = pd.isna(col1)
        nan_mask2 = pd.isna(col2)
        
        # Check if NaN patterns match
        if not nan_mask1.equals(nan_mask2):
            nan_diff_indices = np.where(nan_mask1 != nan_mask2)[0]
            for idx in nan_diff_indices[:10]:  # Limit to first 10
                differences.append({
                    'column': col_name,
                    'row': int(idx),
                    'value1': col1.iloc[idx] if not pd.isna(col1.iloc[idx]) else 'NaN',
                    'value2': col2.iloc[idx] if not pd.isna(col2.iloc[idx]) else 'NaN',
                    'type': 'null_mismatch'
                })
        
        # Compare non-NaN values
        valid_mask = ~(nan_mask1 | nan_mask2)
        if valid_mask.any():
            col1_valid = col1[valid_mask]
            col2_valid = col2[valid_mask]
            
            if pd.api.types.is_numeric_dtype(col1_valid) and pd.api.types.is_numeric_dtype(col2_valid):
                # Numeric comparison with tolerance
                if not np.allclose(col1_valid.astype(float), col2_valid.astype(float), 
                                 rtol=self.tolerance, atol=self.tolerance, equal_nan=True):
                    diff_mask = ~np.isclose(col1_valid.astype(float), col2_valid.astype(float),
                                          rtol=self.tolerance, atol=self.tolerance, equal_nan=True)
                    diff_indices = np.where(valid_mask)[0][diff_mask]
                    
                    for idx in diff_indices[:10]:  # Limit to first 10
                        differences.append({
                            'column': col_name,
                            'row': int(idx),
                            'value1': float(col1.iloc[idx]),
                            'value2': float(col2.iloc[idx]),
                            'difference': abs(float(col1.iloc[idx]) - float(col2.iloc[idx])),
                            'type': 'numeric_difference'
                        })
            else:
                # String comparison
                if not col1_valid.equals(col2_valid):
                    diff_mask = col1_valid != col2_valid
                    diff_indices = np.where(valid_mask)[0][diff_mask]
                    
                    for idx in diff_indices[:10]:  # Limit to first 10
                        differences.append({
                            'column': col_name,
                            'row': int(idx),
                            'value1': str(col1.iloc[idx]),
                            'value2': str(col2.iloc[idx]),
                            'type': 'string_difference'
                        })
        
        return differences
    
    def compare_process_files(self, process_id1: str, process_id2: str) -> Dict:
        """
        Compare all CSV files between two processes.
        
        Args:
            process_id1, process_id2: Process IDs to compare
            
        Returns:
            Dictionary with comparison results for all CSV types
        """
        files_by_process = self.discover_csv_files()
        
        if process_id1 not in files_by_process:
            raise ValueError(f"Process ID {process_id1} not found")
        if process_id2 not in files_by_process:
            raise ValueError(f"Process ID {process_id2} not found")
        
        results = {
            'process_ids': (process_id1, process_id2),
            'csv_comparisons': {},
            'overall_identical': True
        }
        
        for csv_type in self.csv_types:
            if (csv_type in files_by_process[process_id1] and 
                csv_type in files_by_process[process_id2]):
                
                file1 = files_by_process[process_id1][csv_type][0]  # Take first file
                file2 = files_by_process[process_id2][csv_type][0]  # Take first file
                
                print(f"Comparing {csv_type}: {Path(file1).name} vs {Path(file2).name}")
                
                try:
                    df1 = pd.read_csv(file1)
                    df2 = pd.read_csv(file2)
                    
                    comparison_result = self.compare_dataframes(df1, df2, 
                                                              Path(file1).name, 
                                                              Path(file2).name)
                    results['csv_comparisons'][csv_type] = comparison_result
                    
                    if not comparison_result['identical']:
                        results['overall_identical'] = False
                        
                except Exception as e:
                    results['csv_comparisons'][csv_type] = {
                        'error': f"Failed to compare {csv_type}: {str(e)}"
                    }
                    results['overall_identical'] = False
            else:
                missing_in = []
                if csv_type not in files_by_process[process_id1]:
                    missing_in.append(process_id1)
                if csv_type not in files_by_process[process_id2]:
                    missing_in.append(process_id2)
                
                results['csv_comparisons'][csv_type] = {
                    'error': f"{csv_type} missing in process(es): {', '.join(missing_in)}"
                }
                results['overall_identical'] = False
        
    def compare_all_matching_processes(self) -> Dict:
        """
        Compare all matching process IDs between Python and R folders.
        
        Returns:
            Dictionary with comparison results for all matching processes
        """
        files_by_process = self.discover_csv_files()
        
        # Group files by folder origin
        python_processes = {}
        r_processes = {}
        
        for process_id, csv_types in files_by_process.items():
            for csv_type, file_paths in csv_types.items():
                for file_path in file_paths:
                    file_path_obj = Path(file_path)
                    
                    # Determine if file is from Python or R folder
                    if self.python_folder and self.python_folder in file_path_obj.parents:
                        if process_id not in python_processes:
                            python_processes[process_id] = {}
                        if csv_type not in python_processes[process_id]:
                            python_processes[process_id][csv_type] = []
                        python_processes[process_id][csv_type].append(file_path)
                    elif self.r_folder and self.r_folder in file_path_obj.parents:
                        if process_id not in r_processes:
                            r_processes[process_id] = {}
                        if csv_type not in r_processes[process_id]:
                            r_processes[process_id][csv_type] = []
                        r_processes[process_id][csv_type].append(file_path)
        
        # Find matching process IDs
        matching_processes = set(python_processes.keys()) & set(r_processes.keys())
        
        if not matching_processes:
            return {
                'error': 'No matching process IDs found between Python and R folders',
                'python_processes': list(python_processes.keys()),
                'r_processes': list(r_processes.keys())
            }
        
        # Compare all matching processes
        all_results = {
            'matching_processes': sorted(matching_processes),
            'total_processes': len(matching_processes),
            'all_identical': True,
            'process_results': {}
        }
        
        for process_id in sorted(matching_processes):
            print(f"\n🔄 Comparing Process {process_id}...")
            
            process_result = {
                'process_id': process_id,
                'csv_comparisons': {},
                'process_identical': True
            }
            
            for csv_type in self.csv_types:
                if (csv_type in python_processes[process_id] and 
                    csv_type in r_processes[process_id]):
                    
                    python_file = python_processes[process_id][csv_type][0]
                    r_file = r_processes[process_id][csv_type][0]
                    
                    print(f"  📊 {csv_type}: {Path(python_file).name} vs {Path(r_file).name}")
                    
                    try:
                        df_python = pd.read_csv(python_file)
                        df_r = pd.read_csv(r_file)
                        
                        comparison_result = self.compare_dataframes(df_python, df_r,
                                                                  f"Python/{Path(python_file).name}",
                                                                  f"R/{Path(r_file).name}")
                        process_result['csv_comparisons'][csv_type] = comparison_result
                        
                        if not comparison_result['identical']:
                            process_result['process_identical'] = False
                            all_results['all_identical'] = False
                            print(f"    ❌ Differences found")
                        else:
                            print(f"    ✅ Identical")
                            
                    except Exception as e:
                        process_result['csv_comparisons'][csv_type] = {
                            'error': f"Failed to compare {csv_type}: {str(e)}"
                        }
                        process_result['process_identical'] = False
                        all_results['all_identical'] = False
                        print(f"    ❌ Error: {str(e)}")
                else:
                    missing_in = []
                    if csv_type not in python_processes[process_id]:
                        missing_in.append("Python")
                    if csv_type not in r_processes[process_id]:
                        missing_in.append("R")
                    
                    process_result['csv_comparisons'][csv_type] = {
                        'error': f"{csv_type} missing in: {', '.join(missing_in)}"
                    }
                    process_result['process_identical'] = False
                    all_results['all_identical'] = False
                    print(f"    ❌ Missing in: {', '.join(missing_in)}")
            
            all_results['process_results'][process_id] = process_result
        
        return all_results
    
    def print_bulk_comparison_report(self, results: Dict):
        """
        Print a summary report for bulk comparison of all processes.
        
        Args:
            results: Results from compare_all_matching_processes
        """
        if 'error' in results:
            print(f"\n❌ ERROR: {results['error']}")
            print(f"Python processes found: {results.get('python_processes', [])}")
            print(f"R processes found: {results.get('r_processes', [])}")
            return
        
        print(f"\n{'='*100}")
        print(f"BULK CSV COMPARISON REPORT - PYTHON vs R")
        print(f"{'='*100}")
        
        if results['all_identical']:
            print(f"🎉 ALL {results['total_processes']} PROCESSES ARE IDENTICAL!")
        else:
            identical_count = sum(1 for r in results['process_results'].values() if r['process_identical'])
            print(f"⚠️  {identical_count}/{results['total_processes']} processes are identical")
        
        print(f"\nProcesses compared: {', '.join(results['matching_processes'])}")
        
        # Summary table
        print(f"\n{'Process ID':<12} {'Status':<10} {'df_calendario':<15} {'df_colaborador':<15} {'df_estimativas':<15}")
        print(f"{'-'*12} {'-'*10} {'-'*15} {'-'*15} {'-'*15}")
        
        for process_id in sorted(results['matching_processes']):
            process_result = results['process_results'][process_id]
            status = "✅ PASS" if process_result['process_identical'] else "❌ FAIL"
            
            csv_statuses = []
            for csv_type in self.csv_types:
                csv_type_short = csv_type.replace('df_', '')
                if csv_type in process_result['csv_comparisons']:
                    comp_result = process_result['csv_comparisons'][csv_type]
                    if 'error' in comp_result:
                        csv_statuses.append(f"❌ ERROR")
                    elif comp_result['identical']:
                        csv_statuses.append(f"✅ OK")
                    else:
                        diff_count = comp_result['summary'].get('total_differences', '?')
                        csv_statuses.append(f"❌ {diff_count} diff")
                else:
                    csv_statuses.append("❌ MISSING")
            
            print(f"{process_id:<12} {status:<10} {csv_statuses[0]:<15} {csv_statuses[1]:<15} {csv_statuses[2]:<15}")
        
        # Detailed differences for failed processes
        failed_processes = [pid for pid, result in results['process_results'].items() 
                          if not result['process_identical']]
        
        if failed_processes:
            print(f"\n{'='*100}")
            print(f"DETAILED DIFFERENCES")
            print(f"{'='*100}")
            
            for process_id in failed_processes:
                process_result = results['process_results'][process_id]
                print(f"\n🔍 Process {process_id} - Detailed Issues:")
                
                for csv_type, comparison in process_result['csv_comparisons'].items():
                    if 'error' in comparison:
                        print(f"   📁 {csv_type}: ❌ {comparison['error']}")
                        continue
                    
                    if not comparison['identical']:
                        print(f"   📁 {csv_type}: ❌ {comparison['summary'].get('total_differences', 0)} differences")
                        
                        # Show sample differences
                        for diff in comparison['differences']:
                            if diff['type'] == 'value_differences':
                                print(f"      🔸 Sample differences (first 3):")
                                for i, vdiff in enumerate(diff['details'][:3]):
                                    if vdiff['type'] == 'numeric_difference':
                                        print(f"         Row {vdiff['row']}, {vdiff['column']}: {vdiff['value1']} vs {vdiff['value2']} (diff: {vdiff['difference']:.2e})")
                                    else:
                                        print(f"         Row {vdiff['row']}, {vdiff['column']}: '{vdiff['value1']}' vs '{vdiff['value2']}'")
        
        print(f"\n{'='*100}")

    def print_comparison_report(self, results: Dict):
        """
        Print a detailed comparison report.
        
        Args:
            results: Results from compare_process_files
        """
        process_id1, process_id2 = results['process_ids']
        
        print(f"\n{'='*80}")
        print(f"CSV COMPARISON REPORT")
        print(f"Process {process_id1} vs Process {process_id2}")
        print(f"{'='*80}")
        
        if results['overall_identical']:
            print(f"✅ ALL CSV FILES ARE IDENTICAL!")
        else:
            print(f"❌ DIFFERENCES FOUND")
        
        print(f"\nDetailed Results:")
        print(f"{'-'*80}")
        
        for csv_type, comparison in results['csv_comparisons'].items():
            print(f"\n📁 {csv_type.upper()}")
            
            if 'error' in comparison:
                print(f"   ❌ ERROR: {comparison['error']}")
                continue
            
            if comparison['identical']:
                print(f"   ✅ IDENTICAL")
                print(f"   📊 Shape: {comparison['summary']['df1_shape']}")
            else:
                print(f"   ❌ DIFFERENCES FOUND")
                print(f"   📊 Shapes: {comparison['summary']['df1_shape']} vs {comparison['summary']['df2_shape']}")
                
                if 'total_differences' in comparison['summary']:
                    print(f"   🔢 Total differences: {comparison['summary']['total_differences']}")
                
                # Show sample differences
                for diff in comparison['differences']:
                    if diff['type'] == 'shape_mismatch':
                        print(f"   🔸 {diff['details']}")
                    elif diff['type'] == 'column_mismatch':
                        if diff['details']['missing_in_df2']:
                            print(f"   🔸 Columns missing in file 2: {diff['details']['missing_in_df2']}")
                        if diff['details']['missing_in_df1']:
                            print(f"   🔸 Columns missing in file 1: {diff['details']['missing_in_df1']}")
                    elif diff['type'] == 'value_differences':
                        print(f"   🔸 Sample value differences (showing first 5):")
                        for i, vdiff in enumerate(diff['details'][:5]):
                            if vdiff['type'] == 'numeric_difference':
                                print(f"      Row {vdiff['row']}, {vdiff['column']}: {vdiff['value1']} vs {vdiff['value2']} (diff: {vdiff['difference']:.2e})")
                            else:
                                print(f"      Row {vdiff['row']}, {vdiff['column']}: '{vdiff['value1']}' vs '{vdiff['value2']}'")
        
        print(f"\n{'='*80}")


def main():
    """
    Main function to run the CSV comparison tool.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Compare CSV files between different processes')
    
    # Create subparsers for different modes
    subparsers = parser.add_subparsers(dest='mode', help='Comparison mode')
    
    # Single comparison mode
    single_parser = subparsers.add_parser('single', help='Compare two specific process IDs')
    single_parser.add_argument('process_id1', help='First process ID to compare')
    single_parser.add_argument('process_id2', help='Second process ID to compare')
    single_parser.add_argument('--directory', '-d', default='.', help='Directory containing CSV files')
    
    # Bulk comparison mode
    bulk_parser = subparsers.add_parser('bulk', help='Compare all matching processes between Python and R folders')
    bulk_parser.add_argument('python_folder', help='Folder containing Python-generated CSV files')
    bulk_parser.add_argument('r_folder', help='Folder containing R-generated CSV files')
    
    # Common arguments
    for sub_parser in [single_parser, bulk_parser]:
        sub_parser.add_argument('--tolerance', '-t', type=float, default=1e-10, 
                               help='Numerical tolerance for floating point comparisons')
    
    # If no arguments provided, show help
    if len(sys.argv) == 1:
        parser.print_help()
        return
    
    args = parser.parse_args()
    
    if args.mode == 'single':
        # Single comparison mode
        comparator = CSVProcessComparator(args.directory, args.tolerance)
        
        # Discover files
        files_found = comparator.discover_csv_files()
        print(f"Found CSV files for {len(files_found)} processes:")
        for pid, types in files_found.items():
            print(f"  Process {pid}: {list(types.keys())}")
        
        # Run comparison
        try:
            results = comparator.compare_process_files(args.process_id1, args.process_id2)
            comparator.print_comparison_report(results)
            
            # Exit with appropriate code
            exit_code = 0 if results['overall_identical'] else 1
            exit(exit_code)
            
        except Exception as e:
            print(f"Error: {str(e)}")
            exit(1)
    
    elif args.mode == 'bulk':
        # Bulk comparison mode
        comparator = CSVProcessComparator(tolerance=args.tolerance, 
                                        python_folder=args.python_folder, 
                                        r_folder=args.r_folder)
        
        print(f"🔍 Searching for CSV files...")
        print(f"   Python folder: {args.python_folder}")
        print(f"   R folder: {args.r_folder}")
        
        # Run bulk comparison
        try:
            results = comparator.compare_all_matching_processes()
            comparator.print_bulk_comparison_report(results)
            
            # Exit with appropriate code
            if 'error' in results:
                exit(1)
            else:
                exit_code = 0 if results['all_identical'] else 1
                exit(exit_code)
                
        except Exception as e:
            print(f"Error: {str(e)}")
            exit(1)
    
    else:
        parser.print_help()


if __name__ == "__main__":
    import sys
    main()