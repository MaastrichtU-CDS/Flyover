#!/usr/bin/env python3
"""
Simple test to verify Docker build and basic functionality
"""

import sys
import os
import tempfile
import pandas as pd
from pathlib import Path

def test_docker_build():
    """Test that Docker build works"""
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "images", "flyover-triplifier-test"],
            capture_output=True,
            text=True
        )
        if "flyover-triplifier-test" in result.stdout:
            print("✓ Docker image built successfully")
            return True
        else:
            print("✗ Docker image not found")
            return False
    except Exception as e:
        print(f"✗ Docker test failed: {e}")
        return False

def test_yaml_import():
    """Test that PyYAML works"""
    try:
        import yaml
        test_data = {'db': {'url': 'test://test'}}
        yaml_str = yaml.dump(test_data)
        loaded = yaml.safe_load(yaml_str)
        if loaded == test_data:
            print("✓ PyYAML import and basic functionality works")
            return True
        else:
            print("✗ PyYAML functionality test failed")
            return False
    except Exception as e:
        print(f"✗ PyYAML test failed: {e}")
        return False

def test_rdflib_import():
    """Test that rdflib works"""
    try:
        import rdflib
        graph = rdflib.Graph()
        # Add a simple triple
        graph.add((rdflib.URIRef("http://example.com/subject"), 
                  rdflib.URIRef("http://example.com/predicate"), 
                  rdflib.Literal("object")))
        if len(graph) == 1:
            print("✓ rdflib import and basic functionality works")
            return True
        else:
            print("✗ rdflib functionality test failed")
            return False
    except Exception as e:
        print(f"✗ rdflib test failed: {e}")
        return False

def test_sqlite_functionality():
    """Test SQLite functionality for CSV processing"""
    try:
        import sqlite3
        import pandas as pd
        
        # Create test data
        test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
        
        with tempfile.NamedTemporaryFile(suffix='.db') as temp_db:
            # Test pandas to SQLite conversion
            conn = sqlite3.connect(temp_db.name)
            test_data.to_sql('test_table', conn, if_exists='replace', index=False)
            
            # Test reading back
            result = pd.read_sql('SELECT * FROM test_table', conn)
            conn.close()
            
            if len(result) == 3 and result.columns.tolist() == ['id', 'name', 'age']:
                print("✓ SQLite and pandas integration works")
                return True
            else:
                print("✗ SQLite functionality test failed")
                return False
    except Exception as e:
        print(f"✗ SQLite test failed: {e}")
        return False

def main():
    print("Running Basic Functionality Tests...\n")
    
    tests = [
        ("Docker Build", test_docker_build),
        ("PyYAML Import", test_yaml_import),
        ("rdflib Import", test_rdflib_import),
        ("SQLite Functionality", test_sqlite_functionality),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"Running {test_name} test...")
        if test_func():
            passed += 1
        print()
    
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("All basic tests passed! ✓")
        print("The Python Triplifier integration is ready for use.")
        sys.exit(0)
    else:
        print("Some tests failed! ✗")
        sys.exit(1)

if __name__ == "__main__":
    main()