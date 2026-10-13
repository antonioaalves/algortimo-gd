project_structure = {
    "stages": {
        "data_loading": {
            "sequence": 1,               # Stage order
            "requires_previous": False,  # First stage doesn't require previous stages
            "validation_required": True, # Validate data after loading (normally performed by a method associated with the data container in src/models.py)
            "decisions": {
                "selections": {          # Decision point for data selection
                    "selected_entities": ["valid_employees"],  # Default entities to load
                    "load_all": False,   # Whether to load all available entities
                },
                "params_algo": {
                    "param_pessoas_objetivo": 0.5,
                    "param_NLDF": 2,
                    "param_NL10": 0,
                    "param_relax_colabs": 2
                }, 
            }
        },
        
        "processing": {
            "sequence": 2,
            "requires_previous": True,   # Requires previous stage completion
            "validation_required": True,
            "decisions": {
                "transformations": {     # Decision point for transformation options
                    "apply_filtering": False,
                    "filter_column": "",
                    "filter_value": "",
                    "normalize_numeric": True,  # Whether to normalize numerical data
                    "fill_missing": True,       # Whether to fill missing values
                    "fill_method": "mean"       # Method for filling missing values
                },
                #'algorithm': {
                #    'name': 'salsa_algorithm',  # Default algorithm to use - should come from the parameters_defaults in the future
                #    'parameters': {}
                #},
                "insertions": {
                    "insert_results": True
                }
            },
            "substages": {
                "treat_params": {
                    "sequence": 1,
                    "description": "Establishing connection to data source",
                    "required": True,
                    "decisions": {}
                },
                "load_matrices": {
                    "sequence": 2,
                    "description": "Load dataframes containing all the data",
                    "required": True,
                    "decisions": {}                  
                },
                "func_inicializa": {
                    "sequence": 3,
                    "description": "Function that initializes data transformation for each matrix",
                    "required": True,
                    "decisions": {}                     
                },
                "allocation_cycle": {
                    "sequence": 4,
                    "description": "Allocation cycle for all the required days.",
                    "required": True,
                    "decisions": {
                        "algorithms": ["salsa_algorithm"]
                    }                     
                },
                "format_results": {
                    "sequence": 5,
                    "description": "Format results to be inserted",
                    "required": True,
                    "decisions": {}
                },
                "insert_results": {
                    "sequence": 6,
                    "description": "Insert results to the database",
                    "required": False,
                    "decisions": {}
                }
            },
            "auto_complete_on_substages": False, # Auto-complete stage when all substages are done
        },
        

    }
}