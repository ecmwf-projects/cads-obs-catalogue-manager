"""Handles the logging configuration.

It sets two loggers, the root and the project root "cdsobs" with a common default
handler.
"""
import logging
import logging.config
import os
import sys
from typing import Any, Literal

import structlog

LogLevel = Literal["NOTSET", "DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]


# Simple handler that just sets a flag if a warning is logged
class WarningFlagHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.warning_logged = False
        self.records = set()

    def emit(self, record):
        if record.levelno == logging.WARNING:
            self.warning_logged = True
            # We remove the first part of the log record because it has the timestamp,
            # and we do not want to record repeated warnings.
            message = (
                record.msg.split("[warning  ]")[1]
                + f" in line {record.lineno} in {record.pathname}"
            )
            self.records.add(message)


def sizeof_fmt(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f} Yi{suffix}"


def configure_logger() -> WarningFlagHandler:
    """Configure the logging module.

    This function configures the logging module to log in rfc5424 format.
    """
    logging_level = os.environ.get("CADSOBS_LOGGING_LEVEL", "INFO")
    logging.basicConfig(
        level=logging.getLevelName(logging_level),
        format="%(message)s",
        stream=sys.stdout,
    )
    # Add a warning tracker to the root logger
    warning_tracker = WarningFlagHandler()
    root_logger = logging.getLogger()
    root_logger.addHandler(warning_tracker)
    logging_format = os.environ.get("CADSOBS_LOGGING_FORMAT", "CONSOLE")
    if logging_format == "CONSOLE":
        renderer = structlog.dev.ConsoleRenderer(colors=False)
    elif logging_format == "JSON":
        renderer = structlog.processors.JSONRenderer()  # type: ignore
    else:
        raise KeyError(
            f"Unknown value for CADSOBS_LOGGING_FORMAT {logging_format}, use CONSOLE or JSON."
        )
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="ISO", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    return warning_tracker


def get_logger(name: str) -> Any:
    return structlog.get_logger(name)
