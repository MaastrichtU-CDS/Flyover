#!/usr/bin/env python3
"""
Script to setup test data and access annotation UI
"""

import requests
import json

# Setup test data
semantic_map = {
    "endpoint": "http://localhost:7200/repositories/userRepo/statements",
    "database_name": "test_database",
    "prefixes": "PREFIX db: <http://data.local/> PREFIX dbo: <http://um-cds/ontologies/databaseontology/>",
    "variable_info": {
        "biological_sex": {
            "predicate": "roo:P100018",
            "class": "ncit:C28421",
            "local_definition": "sex",
            "data_type": "categorical",
            "value_mapping": {
                "terms": {
                    "male": {"local_term": "M", "target_class": "ncit:C20197"},
                    "female": {"local_term": "F", "target_class": "ncit:C16576"}
                }
            }
        },
        "age_at_inclusion": {
            "predicate": "roo:P100008",
            "class": "ncit:C25150",
            "local_definition": "age",
            "data_type": "continuous"
        }
    }
}

# Save as test file
with open('/tmp/test_semantic_map.json', 'w') as f:
    json.dump(semantic_map, f, indent=2)

print("✅ Test semantic map created at /tmp/test_semantic_map.json")
print("📄 Contents:")
print(json.dumps(semantic_map, indent=2))