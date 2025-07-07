#!/usr/bin/env python3
"""
Controlled test script to debug query execution using the data manager.
This script isolates the query execution to help identify the exact issue.
"""

import os
import sys
import traceback
from pathlib import Path

# Add the project root to the path so we can import modules
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

def test_query_execution():
    """Test query execution in isolation"""
    
    print("=== Query Execution Test ===")
    
    try:
        # Import your config
        from src.config import CONFIG
        print("✓ Config imported successfully")
        
        # Import the data manager factory
        from base_data_project.utils import create_components
        print("✓ Data manager factory imported successfully")
        
        # Create data manager
        print("\n1. Creating data manager...")
        data_manager, _ = create_components(use_db=True, config=CONFIG)
        print("✓ Data manager created successfully")
        
        # Test connection
        print("\n2. Testing database connection...")
        with data_manager:
            print("✓ Database connection established")
            
            # Test basic query first
            print("\n3. Testing basic database query...")
            try:
                # Simple Oracle test query
                basic_result = data_manager.session.execute("SELECT 1 FROM DUAL")
                basic_data = basic_result.fetchone()
                print(f"✓ Basic query successful: {basic_data}")
            except Exception as e:
                print(f"✗ Basic query failed: {e}")
                return False
            
            # Get the specific entity and query file we're testing
            print("\n4. Testing specific entity query...")
            entities = CONFIG.get('available_entities_processing', {})
            entity_name = 'valid_emp'  # The one that's failing
            
            if entity_name not in entities:
                print(f"✗ Entity '{entity_name}' not found in config")
                return False
                
            query_file_path = entities[entity_name]
            print(f"Entity: {entity_name}")
            print(f"Query file path: {query_file_path}")
            
            # Check if query file exists
            print("\n5. Checking query file...")
            if not os.path.exists(query_file_path):
                print(f"✗ Query file does not exist: {query_file_path}")
                return False
            print("✓ Query file exists")
            
            # Read query file content
            print("\n6. Reading query file content...")
            try:
                with open(query_file_path, 'r', encoding='utf-8') as f:
                    query_content = f.read().strip()
                
                if not query_content:
                    print("✗ Query file is empty")
                    return False
                    
                print(f"✓ Query file read successfully")
                print(f"Query length: {len(query_content)} characters")
                print(f"First 200 characters: {query_content[:200]}...")
                
                # Check for common issues
                if ':' in query_content and 'bind' not in query_content.lower():
                    print("⚠ WARNING: Query contains ':' - might have bind variables")
                
            except Exception as e:
                print(f"✗ Error reading query file: {e}")
                return False
            
            # Test query execution manually
            print("\n7. Testing manual query execution...")
            try:
                from sqlalchemy import text
                
                # Execute the query
                result = data_manager.session.execute(text(query_content))
                rows = result.fetchall()
                columns = list(result.keys()) if hasattr(result, 'keys') else []
                
                print(f"✓ Query executed successfully")
                print(f"Rows returned: {len(rows)}")
                print(f"Columns: {columns}")
                
                if rows:
                    print(f"Sample row: {dict(zip(columns, rows[0])) if columns else rows[0]}")
                
            except Exception as e:
                print(f"✗ Query execution failed: {e}")
                print(f"Error type: {type(e).__name__}")
                print(f"Full traceback:")
                traceback.print_exc()
                return False
            
            # Test using data manager's load_data method
            print("\n8. Testing data manager load_data method...")
            try:
                df = data_manager.load_data(
                    entity=entity_name,
                    query_file=query_file_path
                )
                
                print(f"✓ Data manager load_data successful")
                print(f"DataFrame shape: {df.shape}")
                print(f"DataFrame columns: {list(df.columns)}")
                
                if not df.empty:
                    print(f"Sample data:")
                    print(df.head(2))
                
            except Exception as e:
                print(f"✗ Data manager load_data failed: {e}")
                print(f"Error type: {type(e).__name__}")
                print(f"Full traceback:")
                traceback.print_exc()
                return False
            
        print("\n=== ALL TESTS PASSED ===")
        return True
        
    except ImportError as e:
        print(f"✗ Import error: {e}")
        print("Make sure all required modules are installed and paths are correct")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        print(f"Error type: {type(e).__name__}")
        print(f"Full traceback:")
        traceback.print_exc()
        return False

def test_config_paths():
    """Test that all configured paths exist"""
    
    print("\n=== Config Paths Test ===")
    
    try:
        from src.config import CONFIG
        
        entities = CONFIG.get('available_entities_processing', {})
        
        for entity_name, query_path in entities.items():
            print(f"\nEntity: {entity_name}")
            print(f"Path: {query_path}")
            
            if not query_path:
                print("✗ Path is empty!")
                continue
                
            if os.path.exists(query_path):
                print("✓ File exists")
                
                # Check file size
                file_size = os.path.getsize(query_path)
                print(f"File size: {file_size} bytes")
                
                if file_size == 0:
                    print("✗ File is empty!")
                else:
                    print("✓ File has content")
            else:
                print("✗ File does not exist!")
                
    except Exception as e:
        print(f"✗ Error testing config paths: {e}")

def test_database_connection_only():
    """Test just the database connection"""
    
    print("\n=== Database Connection Only Test ===")
    
    try:
        from src.config import CONFIG
        from base_data_project.utils import create_components
        
        print("Creating data manager...")
        data_manager, _ = create_components(use_db=True, config=CONFIG)
        
        print("Testing connection...")
        with data_manager:
            print("✓ Connection successful")
            
            # Test a simple query
            from sqlalchemy import text
            result = data_manager.session.execute(text("SELECT SYSDATE FROM DUAL"))
            current_time = result.fetchone()
            print(f"✓ Database time: {current_time[0]}")
            
        return True
        
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        print(f"Full traceback:")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Starting controlled query execution tests...\n")
    
    # Test 1: Check config paths
    test_config_paths()
    
    # Test 2: Test database connection only
    connection_ok = test_database_connection_only()
    
    if connection_ok:
        # Test 3: Full query execution test
        test_query_execution()
    else:
        print("\n❌ Skipping query tests due to connection failure")
    
    print("\nTest completed.")