SOLVERS = {
    "salsa": {
        "name": "salsa",
        "data_sources": {
            "calendario": "data/matriz_calendario_salsa.csv",
            "estimativas": "data/matriz_estimativas_salsa.csv",
            "colaboradores": "data/matriz_colaborador_salsa.csv",
        },
        "max_continuous_working_days": 5,
        "shifts": ["M", "T", "L", "LQ", "LD", "F"],
        "check_shifts": ['M', 'T', 'L', 'LQ'],
        "working_shifts": ["M", "T"],
        "settings":{
            #F days affect c2d and cxx
            "F_special_day": False,
            #defines if we should sum 2 day quality weekends with the number of free sundays
            "free_sundays_plus_c2d": False,
            "missing_days_afect_free_days": False,
        }
    },
    "alcampo": {
        "name": "alcampo",
        "data_sources": {
            "calendario": "data/matriz_calendario_alcampos3.csv",
            "estimativas": "data/matriz_estimativas_alcampos2.csv",
            "colaboradores": "data/matriz_colaborador_alcampos3.csv",
        },
        "max_continuous_working_days": 10,
        "shifts": ["M", "T", "L", "LQ", "F", "V", "LD", "A", "TC"],
        "check_shifts": ['M', 'T', 'L', 'LQ', "LD", "TC"],
        "check_shift_special": ['M', 'T', 'L', "TC"],
        "working_shifts": ["M", "T", "TC"],
        "settings":{
            #F days affect c2d and cxx
            "F_special_day": False,
            #defines if we should sum 2 day quality weekends with the number of free sundays
            "free_sundays_plus_c2d": False,
            "missing_days_afect_free_days": False,
        }
    }
}

# Define which solver to run
SETTINGS = {
    "solver": "alcampo",  # "salsa" or "alcampo"
}