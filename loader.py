# pypi
import dill

# built-in
import sys
import time
import pickle
from pathlib import Path
from collections import defaultdict

# local
from structlogger import configure_logging, get_logger

# directories
INPUT_DATA_PATH_PICKLE = 'data/ohlc.pkl'
# INPUT_DATA_PATH_DILL = 'data/fs_st_div.dill'

# logging
configure_logging(mode='console')
log_main = get_logger(name='loader')

# aux functions

def nested_defaultdict_factory():
    '''Pickle cannot load objects with lambda function, so we need this factory fun.'''
    return defaultdict(dict)

def pickle_load(data_file_path: str):
    log_main.info(f'-- Attempting to load data with PICKLE from {data_file_path} --')
    if not Path(data_file_path).exists():
        log_main.warning('Pickle file not found. Starting with an empty dataset.')
        return defaultdict(nested_defaultdict_factory)

    start_time = time.time()
    try:
        with open(data_file_path, 'rb') as f:
            d = pickle.load(f)
        log_main.info(
            f'Pickle load successful in: {time.time() - start_time:.4f} seconds'
        )
        return d
    except Exception as e:
        log_main.error(f'Failed to load pickle file: {e}. Starting fresh.')
        return defaultdict(nested_defaultdict_factory)

def dill_load(data_file_path: str):
    log_main.info(f'-- Attempting to load data with DILL from {data_file_path} --')
    if not Path(data_file_path).exists():
        log_main.warning('Dill file not found. Cannot load.')
        return None

    start_time = time.time()
    try:
        with open(data_file_path, 'rb') as f:
            d = dill.load(f)
        log_main.info(
            f'Dill load successful in: {time.time() - start_time:.4f} seconds'
        )
        return d
    except Exception as e:
        log_main.exception(f'Failed to load dill file: {e}.')
        return None


if __name__ == '__main__':

    # --- Example of loading data back ---
    log_main.info('--- Verifying saved data ---')
    loaded_pickle = pickle_load(INPUT_DATA_PATH_PICKLE)
    if loaded_pickle:
        log_main.info(f'Pickle loaded {len(loaded_pickle)} records.')
    else:
        log_main.error(f'Cannot load pickle data from {INPUT_DATA_PATH_PICKLE}.')
        sys.exit()

    # loaded_dill = dill_load(INPUT_DATA_PATH_DILL)
    # if loaded_dill:
    #     log_main.info(f'Dill loaded {len(loaded_dill)} records.')

    pass