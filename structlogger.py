"""Structured logger configuration."""

import datetime
import logging
import os
import sys
import threading
from pathlib import Path

import structlog


def configure_logging(log_dir: str = 'jquant_logs', mode: str = None) -> None:
    """Configure structlog for fast file logging or colorized console logging.

    Args:
        log_dir: Directory for log files when mode is "file".
        mode: "file" or "console". If None, reads JQ_LOG_MODE env var. Defaults to "file".
    """
    selected_mode = (mode or os.getenv('JQ_LOG_MODE', 'file')).lower()
    if selected_mode not in {'file', 'console'}:
        selected_mode = 'file'

    handlers = []

    if selected_mode == 'file':
        # Ensure the log directory exists
        Path(log_dir).mkdir(exist_ok=True, parents=True)

        # Generate a timestamped log file name in UTC
        timestamp = datetime.datetime.now(datetime.UTC).strftime('%Y%m%d_%H%M%S')

        # Main application log
        app_log_file = Path(log_dir) / f'app_{timestamp}.log'
        app_handler = logging.FileHandler(app_log_file, mode='a', encoding='utf-8')
        app_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s:%(funcName)s %(message)s'))
        handlers.append(app_handler)

        # Separate file handler for httpx logs
        httpx_log_file = Path(log_dir) / f'httpx_{timestamp}.log'
        httpx_handler = logging.FileHandler(httpx_log_file, mode='a', encoding='utf-8')
        httpx_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s %(message)s'))

        # Separate file handler for exceptions
        exception_log_file = Path(log_dir) / f'errors_{timestamp}.log'
        exception_handler = logging.FileHandler(exception_log_file, mode='a', encoding='utf-8')
        exception_handler.setLevel(logging.ERROR)
        exception_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s %(message)s\n%(exc_info)s'))
        handlers.append(exception_handler)

        # Root logger writes to files
        logging.basicConfig(level=logging.INFO, handlers=handlers, force=True)

        # httpx to its own file, no propagation
        httpx_logger = logging.getLogger('httpx')
        httpx_logger.setLevel(logging.INFO)
        httpx_logger.propagate = False
        httpx_logger.handlers.clear()
        httpx_logger.addHandler(httpx_handler)

        # structlog integrates with stdlib logging (file format handled by logging.Formatter)
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.processors.CallsiteParameterAdder({structlog.processors.CallsiteParameter.FUNC_NAME}),
                structlog.processors.format_exc_info,
                structlog.stdlib.render_to_log_kwargs,
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

    else:
        # Console-only: one stdout handler
        console_handler = logging.StreamHandler(stream=sys.stdout)
        # Let structlog render the whole line, so keep Formatter minimal
        console_handler.setFormatter(logging.Formatter('%(message)s'))
        handlers.append(console_handler)

        logging.basicConfig(level=logging.INFO, handlers=handlers, force=True)

        # Let httpx propagate to root so it appears on console
        httpx_logger = logging.getLogger('httpx')
        httpx_logger.setLevel(logging.INFO)
        httpx_logger.handlers.clear()
        httpx_logger.propagate = True

        # structlog renders colorized, pretty console output
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.processors.CallsiteParameterAdder({structlog.processors.CallsiteParameter.FUNC_NAME}),
                structlog.processors.TimeStamper(fmt='iso', utc=True),
                structlog.processors.format_exc_info,
                structlog.dev.ConsoleRenderer(colors=True),
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

    # Global exception hooks for main thread and worker threads
    root_logger = logging.getLogger()

    def log_unhandled_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        root_logger.critical(
            'unhandled exception',
            exc_info=(exc_type, exc_value, exc_traceback),
        )

    sys.excepthook = log_unhandled_exception

    if hasattr(threading, 'excepthook'):  # Python 3.8+

        def thread_exception_hook(args):
            log_unhandled_exception(args.exc_type, args.exc_value, args.exc_traceback)

        threading.excepthook = thread_exception_hook


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a configured structlog logger with the specified name."""
    return structlog.get_logger(name)
