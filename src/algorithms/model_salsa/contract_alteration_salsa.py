import numpy as np
import pandas as pd
import datetime
from base_data_project.log_config import get_logger
from src.configuration_manager.instance import get_config as get_config_manager

logger = get_logger(get_config_manager().system.project_name)

worker_root = {}
worker_layer = {}
dummy_workers = {}

def get_root(w, worker_parent):
    while w in worker_parent:
        w = worker_parent[w]
    return w

root = get_root(w)



for w in workers_complete:
    ...
    layer = 0
    while triggers not over:
        layer += 1
        change_date = ...
        next_worker_id = max(workers_complete) + 1
        new_w = next_worker_id
        worker_layer[new_w] = layer
        worker_root[new_w] = w

dummy_workers[new_w] = {
    'root': worker_root[w],
    'parent': w,
    'layer': worker_layer[w],
    'change_date': change_date
}