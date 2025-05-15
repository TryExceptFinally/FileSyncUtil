import configparser
import os

from datetime import datetime
from dataclasses import dataclass
from enum import StrEnum

from src.exceptions import ConfigError
from src.logger import LogLevels


@dataclass
class ConfigData:
    # Options
    start_time: datetime.time
    move_older_days: int
    volume_from: int
    volume_to: int
    log_level: LogLevels
    dir_not_found: str
    owner_name: str
    group_name: str
    # Database
    db_name: str
    db_user: str
    db_password: str
    db_host: str
    db_port: int


class ConfigSection(StrEnum):
    options = 'Options'
    database = 'Database'


class Config:
    def __init__(self, ini: str, encoding: str = 'utf-8'):
        self._encoding = encoding

        self.ini = ini
        self.config = configparser.ConfigParser()

    def get_path(self, section: str, option: str, fallback: str = None) -> str:
        path = self.config.get(section, option, fallback=fallback)
        if not path or not os.path.exists(path) or not os.path.isdir(path):
            raise ConfigError(f'{section}:{option} - директория {path} не существует.')
        return path

    def get_time(self, section: str, option: str, fallback: str = None) -> datetime.time:
        time_str = self.config.get(section, option, fallback=fallback)
        try:
            return datetime.strptime(time_str, '%H:%M').time()
        except ValueError:
            raise ConfigError(f'{section}:{option} - {time_str} время указано некорректно. Формат: %H:%M.')

    def read(self) -> ConfigData:
        self.config.read(self.ini, encoding=self._encoding)

        return ConfigData(
            # Options
            log_level=self.config.get(ConfigSection.options, 'log_level', fallback=LogLevels.info),
            start_time=self.get_time(ConfigSection.options, 'start_time', fallback='00:00'),
            move_older_days=self.config.getint(ConfigSection.options, 'move_older_days', fallback=30),
            dir_not_found=self.get_path(ConfigSection.options, 'dir_not_found'),
            volume_from=self.config.getint(ConfigSection.options, 'volume_from'),
            volume_to=self.config.getint(ConfigSection.options, 'volume_to'),
            owner_name=self.config.get(ConfigSection.options, 'owner_name', fallback='makstor'),
            group_name=self.config.get(ConfigSection.options, 'group_name', fallback='makhaon'),
            # Database
            db_name=self.config.get(ConfigSection.database, 'name'),
            db_user=self.config.get(ConfigSection.database, 'user'),
            db_password=self.config.get(ConfigSection.database, 'password'),
            db_host=self.config.get(ConfigSection.database, 'host'),
            db_port=self.config.getint(ConfigSection.database, 'port'),
        )
