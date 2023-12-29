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


class SizeError(Exception):
    pass


class ConfigError(Exception):
    pass


class CatalogueException(Exception):
    pass


def configure_logger() -> None:
    """Configure the logging module.

    This function configures the logging module to log in rfc5424 format.
    """
    logging_level = os.environ.get("CADSOBS_LOGGING_LEVEL", "INFO")
    logging.basicConfig(
        level=logging.getLevelName(logging_level),
        format="%(message)s",
        stream=sys.stdout,
    )
    logging_format = os.environ.get("CADSOBS_LOGGING_FORMAT", "JSON")
    if logging_format == "CONSOLE":
        renderer = structlog.dev.ConsoleRenderer(colors=False)
    elif logging_format == "JSON":
        renderer = structlog.processors.JSONRenderer()  # type: ignore
    else:
        raise KeyError(
            f"Unkownk value for CADSOBS_LOGGING_FORMAT {logging_format}, use CONSOLE or JSON."
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


def get_logger(name: str) -> Any:
    return structlog.get_logger(name)
