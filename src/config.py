import configparser
import os
from dataclasses import dataclass
from typing import Any

from src.exceptions import ConfigError
from src.logger import LogLevels


@dataclass
class ConfigData:
    # Options
    start_time: str
    move_older_days: int
    volume_from: int
    volume_to: int
    log_level: LogLevels
    path_for_not_found: str
    # Database
    db_name: str
    db_user: str
    db_password: str
    db_host: str
    db_port: int


class Config:
    def __init__(self, ini: str, encoding: str = 'utf-8'):
        self._encoding = encoding

        self.ini = ini
        self.config = configparser.ConfigParser()

    def get_path(self, section: str, option: str, fallback: Any = None) -> str:
        path = self.config.get(section, option, fallback=fallback)
        if not path or not os.path.exists(path):
            raise ConfigError(f'{section}:{option} - {path} does not exist')
        return path

    def read(self) -> ConfigData:
        self.config.read(self.ini, encoding=self._encoding)

        return ConfigData(
            # Options
            log_level=self.config.get('Options', 'log_level', fallback=LogLevels.info),
            start_time=self.config.get('Options', 'start_time', fallback='00:00'),
            move_older_days=self.config.getint('Options', 'move_older_days', fallback=30),
            path_for_not_found=self.get_path('Options', 'path_for_not_found'),
            volume_from=self.config.getint('Options', 'volume_from'),
            volume_to=self.config.getint('Options', 'volume_to'),
            # Database
            db_name=self.config.get('Database', 'name'),
            db_user=self.config.get('Database', 'user'),
            db_password=self.config.get('Database', 'password'),
            db_host=self.config.get('Database', 'host'),
            db_port=self.config.getint('Database', 'port'),
        )
