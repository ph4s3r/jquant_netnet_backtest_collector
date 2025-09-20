"""Structured logger configuration."""

import logging
import structlog
from pathlib import Path
import datetime


def configure_logging(log_dir: str = 'jquant_logs') -> None:
    """Configure structlog for fast file logging with string format: datetime level logger func_name message."""
    # ensure the log directory exists
    Path(log_dir).mkdir(exist_ok=True, parents=True)

    # generate a timestamped log file name in utc
    timestamp = datetime.datetime.now(datetime.UTC).strftime('%Y%m%d_%H%M%S')

    # create file handler for main application log
    app_log_file = Path(log_dir) / f'app_{timestamp}.log'
    app_handler = logging.FileHandler(app_log_file, mode='a', encoding='utf-8')
    app_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s %(message)s'))

    # create separate file handler for httpx logs
    httpx_log_file = Path(log_dir) / f'httpx_{timestamp}.log'
    httpx_handler = logging.FileHandler(httpx_log_file, mode='a', encoding='utf-8')
    httpx_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s %(message)s'))

    # configure root logger to write everything except httpx logs to the main file
    logging.basicConfig(level=logging.INFO, handlers=[app_handler])

    # attach dedicated handler for httpx logs and disable propagation to avoid duplicates
    httpx_logger = logging.getLogger('httpx')
    httpx_logger.setLevel(logging.INFO)
    httpx_logger.propagate = False
    httpx_logger.addHandler(httpx_handler)

    # configure structlog to integrate with stdlib logging
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,  # merge context variables
            structlog.stdlib.add_logger_name,  # add logger name to event dict
            structlog.stdlib.add_log_level,  # add log level to event dict
            structlog.processors.CallsiteParameterAdder({structlog.processors.CallsiteParameter.FUNC_NAME}),  # add function name where log was called
            structlog.processors.format_exc_info,  # format exception info if present
            structlog.stdlib.render_to_log_kwargs,  # render event dict into logging kwargs
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a configured structlog logger with the specified name."""
    return structlog.get_logger(name)
