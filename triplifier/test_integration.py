#!/usr/bin/env python3
"""
Simple test script to verify Python Triplifier integration
"""

import sys
import os
import tempfile
import pandas as pd
from pathlib import Path

# Add the project path to sys.path
project_root = Path(__file__).parent / 'data_descriptor'
sys.path.insert(0, str(project_root))

def test_basic_import():
    """Test basic import of our integration module"""
    try:
        from utils.python_triplifier_integration import PythonTriplifierIntegration
        print("✓ Successfully imported PythonTriplifierIntegration")
        return True
    except Exception as e:
        print(f"✗ Failed to import PythonTriplifierIntegration: {e}")
        return False

def test_csv_processing():
    """Test CSV processing with sample data"""
    try:
        from utils.python_triplifier_integration import PythonTriplifierIntegration
        
        # Create test data
        test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            csv_path = f.name
            test_data.to_csv(csv_path, index=False)
        
        # Create a temporary directory for file operations
        with tempfile.TemporaryDirectory() as temp_dir:
            # Initialize integration with temp directory
            integration = PythonTriplifierIntegration('', temp_dir)
            
            # Test CSV processing
            success, message = integration.run_triplifier_csv([test_data], [csv_path])
            
            if success:
                print("✓ CSV processing test passed")
                print(f"  Message: {message}")
            else:
                print("✗ CSV processing test failed")
                print(f"  Error: {message}")
                return False
        
        # Cleanup
        os.unlink(csv_path)
        return True
        
    except Exception as e:
        print(f"✗ CSV processing test failed with exception: {e}")
        return False

def test_yaml_config_creation():
    """Test YAML configuration file creation"""
    try:
        import yaml
        
        config = {
            'db': {
                'url': 'sqlite:///test.db'
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml_path = f.name
            yaml.dump(config, f)
        
        # Read back and verify
        with open(yaml_path, 'r') as f:
            loaded_config = yaml.safe_load(f)
        
        if loaded_config == config:
            print("✓ YAML configuration creation test passed")
            os.unlink(yaml_path)
            return True
        else:
            print("✗ YAML configuration creation test failed")
            os.unlink(yaml_path)
            return False
            
    except Exception as e:
        print(f"✗ YAML configuration test failed with exception: {e}")
        return False

def main():
    print("Running Python Triplifier Integration Tests...\n")
    
    tests = [
        ("Basic Import", test_basic_import),
        ("YAML Config Creation", test_yaml_config_creation),
        ("CSV Processing", test_csv_processing),
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
        print("All tests passed! ✓")
        sys.exit(0)
    else:
        print("Some tests failed! ✗")
        sys.exit(1)

if __name__ == "__main__":
    main()