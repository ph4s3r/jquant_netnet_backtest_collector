"""Performance Logger.

Logs into a csv-style file.
"""

import time
import asyncio
import aiofiles


async def periodic_perf_logger(  # noqa: PLR0913
    period: int,
    perf_log_file: str,
    analysis_date: str,
    semaphore_limit: int,
    tickers_processed_counter: dict,
    stop_event: asyncio.Event,
) -> None:
    """Write performance metrics until stop_event is set."""
    while not stop_event.is_set():
        await asyncio.sleep(period)
        processed = tickers_processed_counter['count']
        duration = time.time() - tickers_processed_counter['start']
        tpm = processed / (duration / 60) if duration > 0 else 0
        async with aiofiles.open(perf_log_file, 'a', encoding='utf-8') as f:
            await f.write(f'{analysis_date},{semaphore_limit},{processed},{duration:.2f},{tpm:.2f}\n')
