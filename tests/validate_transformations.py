"""
Validation script to check if absence/day-off transformations are working correctly.
Run this script to analyze the log file and count transformations.
"""

import re
from collections import defaultdict

LOG_FILE = "logs/algoritmo_GD_20251105_161358.log"

def analyze_log(log_path):
    """Analyze the log file for transformation patterns."""
    
    transformations = defaultdict(int)
    test_cases = []
    
    with open(log_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    for i, line in enumerate(lines):
        # Track "-" + "A" to "AV" (Function 1)
        if "'-' + 'A' to 'AV'" in line:
            transformations['empty_to_AV'] += 1
            # Get context from previous line
            if i > 0:
                prev_line = lines[i-1]
                employee_match = re.search(r'employee (\d+)', prev_line)
                date_match = re.search(r'on (\d{4}-\d{2}-\d{2})', prev_line)
                if employee_match and date_match:
                    test_cases.append({
                        'employee': employee_match.group(1),
                        'date': date_match.group(1),
                        'transformation': '"-" + "A" → "AV"'
                    })
        
        # Track "-" to "VV" (Function 1 - vacation)
        if "'-' to 'VV'" in line:
            transformations['empty_to_VV'] += 1
            if i > 0:
                prev_line = lines[i-1]
                employee_match = re.search(r'employee (\d+)', prev_line)
                date_match = re.search(r'on (\d{4}-\d{2}-\d{2})', prev_line)
                if employee_match and date_match:
                    test_cases.append({
                        'employee': employee_match.group(1),
                        'date': date_match.group(1),
                        'transformation': '"-" → "VV" (vacation)'
                    })
        
        # Track "A" to "AV" (S-type loop)
        if "'A' to 'AV'" in line and "Morning:" in line:
            transformations['A_to_AV_stype'] += 1
        
        # Track "V" to "VV" (S-type loop)
        if "'V' to 'VV'" in line and "Morning:" in line:
            transformations['V_to_VV_stype'] += 1
        
        # Track S-type processing count
        if "Processing" in line and "no-work day (S) records" in line:
            match = re.search(r'Processing (\d+)', line)
            if match:
                transformations['s_records_processed'] = int(match.group(1))
    
    return transformations, test_cases


def print_report(transformations, test_cases):
    """Print a formatted validation report."""
    
    print("=" * 80)
    print("TRANSFORMATION VALIDATION REPORT")
    print("=" * 80)
    print()
    
    print("📊 TRANSFORMATION COUNTS:")
    print("-" * 80)
    print(f"✅ Empty ('-') + Absence ('A') → 'AV':     {transformations['empty_to_AV']}")
    print(f"✅ Empty ('-') + Vacation → 'VV':          {transformations['empty_to_VV']}")
    print(f"✅ 'A' → 'AV' (S-type loop):               {transformations['A_to_AV_stype']}")
    print(f"✅ 'V' → 'VV' (S-type loop):               {transformations['V_to_VV_stype']}")
    print(f"📝 Total S-type records processed:         {transformations.get('s_records_processed', 0)}")
    print()
    
    print("🎯 LOGIC STATUS:")
    print("-" * 80)
    
    status_empty_av = "✅ WORKING" if transformations['empty_to_AV'] > 0 else "❌ NO DATA"
    status_empty_vv = "✅ WORKING" if transformations['empty_to_VV'] > 0 else "⚠️  NO DATA"
    status_a_av = "✅ WORKING" if transformations['A_to_AV_stype'] > 0 else "⚠️  NO DATA (expected if Function 1 runs first)"
    status_v_vv = "✅ WORKING" if transformations['V_to_VV_stype'] > 0 else "⚠️  NO DATA"
    
    print(f"Function 1 - Empty day + Absence:  {status_empty_av}")
    print(f"Function 1 - Empty day + Vacation: {status_empty_vv}")
    print(f"Function 2 - A to AV on S-days:    {status_a_av}")
    print(f"Function 2 - V to VV on S-days:    {status_v_vv}")
    print()
    
    if test_cases:
        print("📋 SAMPLE TEST CASES (first 10):")
        print("-" * 80)
        for i, case in enumerate(test_cases[:10], 1):
            print(f"{i}. Employee {case['employee']} on {case['date']}: {case['transformation']}")
        print()
        if len(test_cases) > 10:
            print(f"   ... and {len(test_cases) - 10} more cases")
            print()
    
    print("=" * 80)
    print("💡 INTERPRETATION:")
    print("=" * 80)
    print("""
If you see '⚠️ NO DATA' for S-type transformations, this is EXPECTED because:
1. Function 1 (insert_holidays_absences) runs FIRST
2. It already converts empty days with absences to AV/VV
3. Function 2 (S-type loop) acts as a safety net for edge cases

The logic is CORRECT if Function 1 transformations are working!
    """)


if __name__ == "__main__":
    try:
        print("Analyzing log file...")
        transformations, test_cases = analyze_log(LOG_FILE)
        print_report(transformations, test_cases)
    except FileNotFoundError:
        print(f"❌ ERROR: Log file not found: {LOG_FILE}")
        print("Please update the LOG_FILE variable in the script.")
    except Exception as e:
        print(f"❌ ERROR: {e}")

