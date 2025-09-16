"""Structured Logger Configuration."""

import logging
import structlog
from pathlib import Path
import datetime


def configure_logging(log_dir: str = 'jquant_logs') -> None:
    """Configure structlog for fast file logging with string format: datetime LEVEL logger func_name message."""
    # Ensure the log directory exists
    Path(log_dir).mkdir(exist_ok=True, parents=True)

    # Generate a timestamped log file name in UTC
    timestamp = datetime.datetime.now(datetime.UTC).strftime('%Y%m%d_%H%M%S')
    log_file = Path(log_dir) / f'app_{timestamp}.log'

    # Create a FileHandler with a Formatter for the desired log format
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s %(message)s'))

    # Configure the standard library logging to write to a file only
    logging.basicConfig(
        level=logging.INFO,  # Set to INFO to suppress DEBUG messages
        handlers=[
            file_handler,  # Log only to file, no console output
        ],
    )

    # Configure structlog to integrate with standard logging
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,  # Merge context variables
            structlog.stdlib.add_logger_name,  # Add logger name
            structlog.stdlib.add_log_level,  # Add log level
            structlog.processors.CallsiteParameterAdder(  # Add function name
                {
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                }
            ),
            structlog.processors.format_exc_info,  # Handle exception info
            structlog.stdlib.render_to_log_kwargs,  # Render to logging kwargs
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a configured structlog logger with the specified name."""
    return structlog.get_logger(name)
