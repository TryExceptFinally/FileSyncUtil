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
main_path = os.path.dirname(__file__)
configure_logging(
    level=config.log_level,
    filename=os.path.join(main_path, 'log.txt'),
)

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
    logger.info(f'Сканирование тома {volume_from_path}.')
    volume_from_dirs = scan_directory(
        volume_from_path,
        exclude_files=True,
        name_filter=lambda name: matches_date_pattern(name, config.move_older_days),
    )

    if not volume_from_dirs:
        logger.info('Не найдено директорий для переноса.')
        return

    logger.info(f'Найдено {len(volume_from_dirs)} директорий.')
    logger.debug(', '.join(v_dir.name for v_dir in volume_from_dirs))

    for v_dir in volume_from_dirs:
        logger.info(f'Сканирование директории {v_dir.name}.')
        dir_files = scan_directory(
            v_dir.path,
            exclude_dirs=True,
        )

        if not dir_files:
            logger.info('Не найдено файлов для переноса.')
            continue

        num_all_files = len(dir_files)

        logger.info(f'Найдено {num_all_files} файлов.')

        num_copied_files = 0
        num_not_removed_files = 0

        for d_file in dir_files:
            logger.debug(f'Извлечение uid из имени файла {d_file.name}')
            image_uid = extract_image_uid_from_name(d_file.name)
            if not image_uid:
                logger.error(f'Файл {d_file.path} не содержит uid.')
                continue

            logger.debug(f'Запрос image из БД по uid={image_uid}')
            image_name = get_image_name_by_uid(db_connector, image_uid)

            if not image_name:
                logger.debug(f'Не удалось получить image из БД по uid. '
                             f'Запрос image из БД по name={d_file.name}.')
                image_uid = get_image_uid_by_name(db_connector, d_file.name)

            if not image_uid:
                logger.error(f'Файл {d_file.path} не найден в БД.')
                continue

            logger.debug(f'Извлечение относительного пути из абсолютного. '
                         f'base_path={volume_from_path}, abs_path={d_file.path}.')
            image_rel_path = extract_rel_path_from_abs_path(
                base_path=volume_from_path, abs_path=d_file.path)

            path_from = d_file.path

            logger.debug(f'Получение абсолютного пути для файла.'
                         f'volume_from_path={volume_from_path}, image_rel_path={image_rel_path}.')
            path_to = str(os.path.join(volume_to_path, image_rel_path))

            if not os.path.exists(path_from):
                logger.error(f'Файл {path_from} отсутствует.')
                continue

            logger.debug(f'Перенос файла {path_from} -> {path_to}.')

            try:
                copy_file(path_from, path_to)
                update_image(
                    db_connector, image_uid=image_uid, share_uid=config.volume_to, image_path=path_to)
                logger.debug(f'Файл успешно скопирован: {path_from} -> {path_to}')
                num_copied_files += 1
            except CopyFileError as err:
                logger.error(f'Не удалось скопировать файл: {path_from} -> {path_to}. Ошибка: {err}')
                continue

            except DBExecuteQueryError as err:
                logger.error(f'Не удалось выполнить запрос в БД. Ошибка: {err}')
                try:
                    remove_file(path_to)
                except RemoveFileError as err:
                    logger.error(f'Не удалось удалить скопированный файл: {path_to}. Ошибка: {err}')
                continue

            try:
                remove_file(path_from)
            except RemoveFileError as err:
                logger.error(f'Не удалось удалить изначальный файл: {path_from}. Ошибка: {err}')
                num_not_removed_files += 1

        logger.info(f'Скопировано всего/Скопировано успешно/Не удалось удалить/Всего: '
                    f'{num_copied_files}/{num_copied_files - num_not_removed_files}/'
                    f'{num_not_removed_files}/{num_all_files}. Директория: {v_dir.name}.')


if __name__ == '__main__':
    logger.info('Программа запущена.')

    try:
        _start_time = datetime.strptime(config.start_time, '%H:%M').time()
    except ValueError as e:
        logger.error(f'Не удалось получить время из: {config.start_time}. Нужный формат: %H:%M.')
        raise ConfigError()

    logger.debug(f'Получение путь до тома источника uid={config.volume_from}.')
    _volume_from_path = get_volume_path(db_connector, config.volume_from)
    if not _volume_from_path:
        logger.error(f'Не удалось найти том с uid={config.volume_from}.')
        raise ConfigError()
    _volume_from_path = _volume_from_path.strip()

    logger.debug(f'Получение путь до целевого тома uid={config.volume_to}.')
    _volume_to_path = get_volume_path(db_connector, config.volume_to)
    if not _volume_to_path:
        logger.error(f'Не удалось найти том с uid={config.volume_to}.')
        raise ConfigError()
    _volume_to_path = _volume_to_path.strip()

    while True:
        if _start_time == datetime.now().time().replace(second=0, microsecond=0):
            logger.info('Запущен перенос файлов.')
            main(_volume_from_path, _volume_to_path)
        sleep(60)
