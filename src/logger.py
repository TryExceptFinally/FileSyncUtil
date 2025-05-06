import logging

from enum import StrEnum


class LogLevels(StrEnum):
    info = 'INFO'
    warn = 'WARN'
    error = 'ERROR'
    debug = 'DEBUG'


def configure_logging(level: str, filename: str):
    log_level = level.upper()

    if log_level not in LogLevels:
        log_level = LogLevels.info

    logging.basicConfig(
        filename=filename,
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
