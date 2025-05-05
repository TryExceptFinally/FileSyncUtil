import logging
import os

from datetime import datetime
from time import sleep

from src.config import Config
from src.database import DatabaseConnector
from src.exceptions import DBExecuteQueryError, RemoveFileError, CopyFileError, ConfigError
from src.logger import configure_logging
from src.service import get_volume_path, update_image, get_image_name_by_uid, get_image_uid_by_name
from src.utils import remove_file, copy_file, scan_directory, matches_date_pattern, \
    extract_image_uid_from_name, extract_rel_path_from_abs_path

# Считываю конфиг
config = Config('config.ini').read()

# Конфигурирую логгинг
configure_logging(config.log_level)

# Конфигурирую подключение к БД
db_connector = DatabaseConnector(
    dbname=config.db_name,
    user=config.db_user,
    password=config.db_password,
    host=config.db_host,
    port=config.db_port,
)

logger = logging.getLogger(__name__)


def main(volume_from_path: str, volume_to_path: str) -> None:
    volume_from_dirs = scan_directory(
        volume_from_path,
        exclude_files=True,
        name_filter=lambda name: matches_date_pattern(name, config.move_older_days),
    )

    if not volume_from_dirs:
        logger.info('Не найдено исследований для переноса.')
        return

    for v_dir in volume_from_dirs:
        dir_files = scan_directory(
            v_dir.path,
            exclude_dirs=True,
        )
        for d_file in dir_files:
            image_uid = extract_image_uid_from_name(d_file.name)
            if not image_uid:
                logger.error(f'Файл {d_file.path} не содержит image_uid.')
                continue

            image_name = get_image_name_by_uid(db_connector, image_uid)

            if not image_name:
                image_uid = get_image_uid_by_name(db_connector, d_file.name)

            if not image_uid:
                logger.error(f'Файл {d_file.path} не найден в БД.')
                continue

            image_rel_path = extract_rel_path_from_abs_path(
                base_path=volume_from_path, abs_path=d_file.path)

            path_from = d_file.path
            path_to = str(os.path.join(volume_to_path, image_rel_path))

            if not os.path.exists(path_from):
                logger.error(f'Файл {path_from} отсутствует.')
                continue

            try:
                copy_file(path_from, path_to)
                update_image(
                    db_connector, image_uid=image_uid, share_uid=config.volume_to, image_path=path_to)
                logger.info(f'Файл успешно перенесен: {path_from} -> {path_to}')

            except CopyFileError as e:
                logger.error(f'Не удалось скопировать файл: {path_from} -> {path_to}. Ошибка: {e}')
                continue

            except DBExecuteQueryError as e:
                logger.error(f'Не удалось выполнить запрос в БД. Ошибка: {e}')
                try:
                    remove_file(path_to)
                except RemoveFileError as e:
                    logger.error(f'Не удалось удалить скопированный файл: {path_to}. Ошибка: {e}')
                continue

            try:
                remove_file(path_from)
            except RemoveFileError as e:
                logger.error(f'Не удалось удалить изначальный файл: {path_from}. Ошибка: {e}')


if __name__ == '__main__':
    _volume_from_path = get_volume_path(db_connector, config.volume_from)
    if not _volume_from_path:
        logger.error(f'Не удалось найти том с uid={config.volume_from}')
        raise ConfigError()
    _volume_from_path = _volume_from_path.strip()

    _volume_to_path = get_volume_path(db_connector, config.volume_to)
    if not _volume_to_path:
        logger.error(f'Не удалось найти том с uid={config.volume_to}')
        raise ConfigError()
    _volume_to_path = _volume_to_path.strip()

    try:
        _start_time = datetime.strptime(config.start_time, '%H:%M').time()
    except ValueError as e:
        logger.error(f'Не удалось получить время из: {config.start_time}. Нужный формат: %H:%M')
        raise ConfigError()

    while True:
        if _start_time == datetime.now().time().replace(second=0, microsecond=0):
            main(_volume_from_path, _volume_to_path)
        sleep(60)
