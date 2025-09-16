"""Structured Logger Configuration."""

import logging
import structlog
from pathlib import Path
from datetime import datetime


def configure_logging(log_dir: str = 'jquant_logs') -> None:
    """Configure structlog for fast file logging with string format: datetime LEVEL logger func_name message."""
    # Ensure the log directory exists
    Path(log_dir).mkdir(exist_ok=True, parents=True)

    # Generate a timestamped log file name
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = Path(log_dir) / Path(f'app{timestamp}.log')

    # Create a FileHandler with a Formatter for the desired log format
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setFormatter(logging.Formatter('%(timestamp)s [%(level)s] %(logger)s:%(func_name)s %(message)s'))

    # Configure the standard library logging to write to a file
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler],
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
            structlog.processors.TimeStamper(fmt='%Y-%m-%d %H:%M:%S'),  # Add timestamp in string format
            structlog.processors.format_exc_info,  # Handle exception info
            structlog.stdlib.render_to_log_kwargs,  # Render to logging kwargs
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str):  # noqa: ANN201
    """Get a configured structlog logger with the specified name."""
    return structlog.get_logger(name)
