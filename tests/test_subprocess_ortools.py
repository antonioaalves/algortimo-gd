#!/usr/bin/env python3
"""
Minimal test to isolate ortools subprocess hanging issue
"""
import subprocess
import os
import sys
from datetime import datetime

def test_direct_ortools():
    """Test ortools directly in current process"""
    print("=== Testing ortools DIRECTLY ===")
    try:
        from ortools.sat.python import cp_model
        model = cp_model.CpModel()
        
        # Create a simple variable and constraint
        x = model.NewIntVar(0, 10, 'x')
        y = model.NewIntVar(0, 10, 'y')
        model.Add(x + y == 5)
        
        # Create solver
        solver = cp_model.CpSolver()
        solver.parameters.num_search_workers = 1
        solver.parameters.max_time_in_seconds = 10
        
        print("Starting direct ortools solve...")
        status = solver.Solve(model)
        print(f"Direct solve status: {status}")
        return True
    except Exception as e:
        print(f"Direct ortools failed: {e}")
        return False

def test_subprocess_ortools():
    """Test ortools in subprocess"""
    print("=== Testing ortools in SUBPROCESS ===")
    
    # Create a minimal script that just runs ortools
    script_content = '''
import sys
sys.path.insert(0, ".")
try:
    print("Subprocess: Importing ortools...")
    from ortools.sat.python import cp_model
    
    print("Subprocess: Creating model...")
    model = cp_model.CpModel()
    
    print("Subprocess: Adding variables...")
    x = model.NewIntVar(0, 10, 'x')
    y = model.NewIntVar(0, 10, 'y')
    model.Add(x + y == 5)
    
    print("Subprocess: Creating solver...")
    solver = cp_model.CpSolver()
    solver.parameters.num_search_workers = 1
    solver.parameters.max_time_in_seconds = 10
    
    print("Subprocess: Starting solve...")
    status = solver.Solve(model)
    print(f"Subprocess: Solve status: {status}")
    print("Subprocess: SUCCESS!")
    
except Exception as e:
    print(f"Subprocess: ERROR: {e}")
    import traceback
    traceback.print_exc()
'''
    
    with open('temp_ortools_test.py', 'w') as f:
        f.write(script_content)
    
    try:
        # Get Python executable path (same as orquestrador.py uses)
        venv_python = os.path.join(os.getcwd(), ".venv", "Scripts", "python.exe")
        
        print(f"Running subprocess with: {venv_python}")
        
        # Run subprocess similar to orquestrador.py
        result = subprocess.run([
            venv_python, 'temp_ortools_test.py'
        ], capture_output=True, text=True, timeout=30)
        
        print("=== SUBPROCESS STDOUT ===")
        print(result.stdout)
        print("=== SUBPROCESS STDERR ===") 
        print(result.stderr)
        print(f"Return code: {result.returncode}")
        
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print("SUBPROCESS TIMED OUT - This reproduces the hanging issue!")
        return False
    except Exception as e:
        print(f"Subprocess test failed: {e}")
        return False
    finally:
        # Clean up
        if os.path.exists('temp_ortools_test.py'):
            os.remove('temp_ortools_test.py')

if __name__ == "__main__":
    print(f"Testing ortools behavior at {datetime.now()}")
    print(f"Working directory: {os.getcwd()}")
    print(f"Python executable: {sys.executable}")
    print()
    
    # Test direct execution
    direct_success = test_direct_ortools()
    print(f"Direct execution: {'SUCCESS' if direct_success else 'FAILED'}")
    print()
    
    # Test subprocess execution  
    subprocess_success = test_subprocess_ortools()
    print(f"Subprocess execution: {'SUCCESS' if subprocess_success else 'FAILED'}")
    print()
    
    if direct_success and not subprocess_success:
        print("CONFIRMED: ortools hangs in subprocess but works directly!")
        print("This isolates the issue to subprocess execution environment.")
    elif direct_success and subprocess_success:
        print("Both work - the issue might be more specific to your full application context.")
    else:
        print("Both failed - might be a general ortools/environment issue.") 