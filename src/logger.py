import logging

from enum import StrEnum


class LogLevels(StrEnum):
    info = 'INFO'
    warn = 'WARN'
    error = 'ERROR'
    debug = 'DEBUG'


def configure_logging(level: str):
    log_level = level.upper()

    if log_level not in LogLevels:
        logging.basicConfig(level=LogLevels.info)
        return

    logging.basicConfig(level=log_level)
