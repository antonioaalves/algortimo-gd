import numpy as np
import pandas as pd
import datetime
from base_data_project.log_config import get_logger
from src.configuration_manager.instance import get_config as get_config_manager

logger = get_logger(get_config_manager().system.project_name)

dummy_workers = {}
worker_with_dummy = {}
workers_complete = ...
workers_complete_with_dummy = workers_complete.copy()
for w in workers_complete:
    ...
    layer = 0
    if trigger:
        worker_with_dummy[w] = {}
        while triggers not over:
            layer += 1
            start_date = ...
            end_date = ...
            new_w = max(workers_complete_with_dummy) + 1
            workers_complete_with_dummy.append(new_w)
            worker_with_dummy[w].append({"dummy": new_w, "date_range": [start_date, end_date]})
            dummy_workers[new_w] = {
                'parent': w,
                'layer': layer,
                'start_date': start_date,
                'end_date': end_date,
            }

workers_complete = workers_complete_with_dummy