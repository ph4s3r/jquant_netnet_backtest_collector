# ruff: noqa

# pypi
import dill

# built-in
import os
import time
import uuid
import pickle
import asyncio
from pathlib import Path
from collections import defaultdict
from asyncio import Semaphore

# local
# Ensure you are using the real client for concurrency to be effective
import jquant_client
from structlogger import configure_logging, get_logger

# --- Configuration ---
LOCAL_LOGDIR = "collector_logs/"
GLACIUS_LOGDIR = r"/var/www/analytics/jq_data_collector/"
GLACIUS_UUID = 94558092206834
ELEMENT_UUID = 91765249380
ON_GLACIUS = GLACIUS_UUID == uuid.getnode()
ULTIMATE_LOGDIR = GLACIUS_LOGDIR if ON_GLACIUS else LOCAL_LOGDIR

OUTPUT_DATA_PATH_PICKLE = "data/fs_st_div.pkl"
OUTPUT_DATA_PATH_DILL = "data/fs_st_div.dill"
INPUT_TICKERS_PATH = "all_tickers/all_tickers.txt"

# --- Concurrency and Checkpointing Configuration ---
SEMAPHORE_LIMIT = 1
BATCH_SIZE = 20

# GLOBAL LOCK FOR APPEND-ONLY WRITES
STREAM_WRITE_LOCK = asyncio.Lock()

configure_logging(log_dir=ULTIMATE_LOGDIR)
log_main = get_logger("main")


def nested_defaultdict_factory():
    """Factory for pickle-friendly nested defaultdict."""
    return defaultdict(dict)


# APPEND MULTIPLE RECORDS (e.g., list of (ticker, data_dict)) TO A BINARY STREAM
def _append_records(saver, records, out_path):
    # records is an iterable of objects, e.g., [(ticker, data), ...]
    try:
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        log_main.info(f"Appending {len(records)} records to {out_path} using {saver.__name__}...")
        start_time = time.time()
        with open(out_path, "ab") as f:
            for rec in records:
                saver(rec, f)
            f.flush()
            os.fsync(f.fileno())
        log_main.info(f"Append to {out_path} finished in: {time.time() - start_time:.4f}s")
    except Exception as e:
        log_main.error(f"Failed to append to {out_path}: {e}")

# NON-BLOCKING APPEND: WRITES TO BOTH PICKLE AND DILL STREAMS
async def append_records_non_blocking(records):
    # records example: [(ticker, {'st': ..., 'fs': ..., 'dv': ...}), ...]
    if not records:
        return
    async with STREAM_WRITE_LOCK:
        loop = asyncio.get_running_loop()
        await asyncio.gather(
            loop.run_in_executor(None, _append_records, pickle.dump, records, OUTPUT_DATA_PATH_PICKLE),
            loop.run_in_executor(None, _append_records, dill.dump, records, OUTPUT_DATA_PATH_DILL)
        )

# STREAM READERS (SEQUENTIAL UNPICKLE UNTIL EOF)
def iter_records(data_file_path, loader):
    if not Path(data_file_path).exists():
        return
    with open(data_file_path, "rb") as f:
        while True:
            try:
                yield loader(f)
            except EOFError:
                break

def iter_pickle_records(data_file_path):
    yield from iter_records(data_file_path, pickle.load)

def iter_dill_records(data_file_path):
    yield from iter_records(data_file_path, dill.load)

# OPTIONAL: RECONSTRUCT CURRENT SNAPSHOT (DICT) FROM APPEND-ONLY PICKLE STREAM
def reconstruct_dataset_from_pickle_stream(stream_path):
    dataset = defaultdict(nested_defaultdict_factory)
    for rec in iter_pickle_records(stream_path):
        if isinstance(rec, tuple) and len(rec) == 2:
            ticker, data = rec
            dataset[ticker] = data
    return dataset

# OPTIONAL: USE THIS INSTEAD OF pickle_load WHEN USING APPEND-ONLY STREAMS
def pickle_stream_load_or_empty(data_file_path: str):
    log_main.info(f"-- Attempting to load append-only stream from {data_file_path} --")
    dataset = defaultdict(nested_defaultdict_factory)
    try:
        for rec in iter_pickle_records(data_file_path):
            if isinstance(rec, tuple) and len(rec) == 2:
                ticker, data = rec
                dataset[ticker] = data
    except Exception as e:
        log_main.error(f"Failed to load append-only stream: {e}. Starting fresh.")
        dataset = defaultdict(nested_defaultdict_factory)
    return dataset



# --- Data Persistence Functions ---

def _save_data(saver, data, out_path):
    """Generic synchronous save function."""
    log_main.info(f"Starting save to {out_path} using {saver.__name__}...")
    start_time = time.time()
    try:
        with open(out_path, "wb") as f:
            if saver == pickle.dump:
                saver(data, f, protocol=pickle.HIGHEST_PROTOCOL)
            else:
                saver(data, f)
        log_main.info(f"Save to {out_path} finished in: {time.time() - start_time:.4f}s")
    except Exception as e:
        log_main.error(f"Failed to save to {out_path}: {e}")

def pickle_load(data_file_path: str):
    log_main.info(f"-- Attempting to load data with PICKLE from {data_file_path} --")
    if not Path(data_file_path).exists():
        log_main.warning("Pickle file not found. Starting with an empty dataset.")
        return defaultdict(nested_defaultdict_factory)
    try:
        with open(data_file_path, "rb") as f:
            return pickle.load(f)
    except Exception as e:
        log_main.error(f"Failed to load pickle file: {e}. Starting fresh.")
        return defaultdict(nested_defaultdict_factory)

async def save_data_non_blocking(data):
    """
    Asynchronously saves data using both pickle and dill without blocking the event loop.
    """
    loop = asyncio.get_running_loop()
    # Run each save operation in a separate thread from the default executor pool
    await asyncio.gather(
        loop.run_in_executor(None, _save_data, pickle.dump, data, OUTPUT_DATA_PATH_PICKLE),
        loop.run_in_executor(None, _save_data, dill.dump, data, OUTPUT_DATA_PATH_DILL)
    )


# --- Concurrent Worker Function (REFINED) ---
async def fetch_data_for_ticker(ticker: str, jquant, semaphore: Semaphore):
    """
    Fetches all data for a single ticker and returns a (ticker, data_dict) tuple.
    Returns None on failure.
    """
    async with semaphore:
        log_main.debug(f"Processing ticker: {ticker}")
        try:
            # Concurrently run the three API calls for the ticker
            successful_results = await asyncio.gather(
                jquant.query_endpoint(endpoint='statements', params={"code": ticker}),
                jquant.query_endpoint(endpoint='fs_details', params={"code": ticker}),
                jquant.query_endpoint(endpoint='dividend', params={"code": ticker}),
                return_exceptions=True # Prevent one failed call from stopping others
            )

            batch_records = [r for r in successful_results if r is not None]

            await append_records_non_blocking(batch_records)

            # Check if any of the API calls failed
            if any(isinstance(res, Exception) for res in successful_results):
                log_main.error(f"One or more API calls failed for ticker {ticker}.")
                return None

            ticker_data = {'st': successful_results[0], 'fs': successful_results[1], 'dv': successful_results[2]}
            return (ticker, ticker_data) # Return a tuple for clean merging

        except Exception as e:
            log_main.error(f"Overall failure fetching data for {ticker}: {e}")
            return None


# --- Main Application Logic (CORRECTED) ---
async def main() -> None:
    # 1. Load initial data
    data = pickle_load(OUTPUT_DATA_PATH_PICKLE)
    log_main.info(f"Starting collection. Already have data for {len(data)} tickers.")

    # 2. Prepare tickers to process
    jquant = jquant_client.JQuantAPIClient()
    all_tickers = [t for t in Path(INPUT_TICKERS_PATH).read_text().split('\n') if t]

    # dummy tickers for testing
    # all_tickers = [random.randint(1000,9999) for _ in range(420)]

    tickers_to_process = [t for t in all_tickers if t not in data]
    log_main.info(f"Found {len(tickers_to_process)} new tickers to process out of {len(all_tickers)} total.")

    if not tickers_to_process:
        log_main.info("No new tickers to process. Exiting.")
        return

    # 3. Set up semaphore and process in batches
    semaphore = Semaphore(SEMAPHORE_LIMIT)
    total_batches = (len(tickers_to_process) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(tickers_to_process), BATCH_SIZE):
        batch_tickers = tickers_to_process[i:i + BATCH_SIZE]
        current_batch_num = i // BATCH_SIZE + 1
        log_main.info(f"--- Starting Batch {current_batch_num}/{total_batches} with {len(batch_tickers)} tickers ---")

        tasks = [fetch_data_for_ticker(ticker, jquant, semaphore) for ticker in batch_tickers]
        batch_results = await asyncio.gather(*tasks)

        # 4. CORRECTED MERGE LOGIC: Update the main data object, don't replace it
        successful_fetches = 0
        for result in batch_results:
            if result:
                ticker_code, ticker_data = result  # Unpack the (ticker, data) tuple
                data[ticker_code] = ticker_data    # Update the main dictionary
                successful_fetches += 1
        
        log_main.info(f"Collected data for {successful_fetches} tickers in this batch.")

        # 5. NON-BLOCKING SAVE: Save checkpoint without freezing
        log_main.info(f"--- Batch complete. Saving checkpoint for {len(data)} total tickers. ---")
        await save_data_non_blocking(data)

    log_main.info("--- All batches processed. Final data collection complete. ---")


if __name__ == "__main__":
    log_main.info("-- STARTING DATA COLLECTION --")
    asyncio.run(main())
    log_main.info("-- FINISHED DATA COLLECTION --")
