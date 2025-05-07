import logging
import os

from src.database import DatabaseConnector
from src.exceptions import CopyFileError, DBExecuteQueryError, RemoveFileError, DBConnectError
from src.service import get_volume_path, get_image_name_by_uid, get_image_uid_by_name, update_image
from src.utils import scan_directory, matches_date_pattern, extract_image_uid_from_name, extract_rel_path_from_abs_path, \
    copy_file, remove_file

logger = logging.getLogger(__name__)


def move_images(
    db_connector: DatabaseConnector,
    volume_from: int,
    volume_to: int,
    move_older_days: int,
):
    logger.debug(f'Получение путь до тома источника uid={volume_from}.')
    try:
        volume_from_path = get_volume_path(db_connector, volume_from)
    except (DBConnectError, DBExecuteQueryError) as err:
        logger.error(f'Не удалось выполнить запрос в БД. Ошибка: {err}')
        return
    if not volume_from_path:
        logger.error(f'Не удалось найти том с uid={volume_from}.')
        return
    volume_from_path = volume_from_path.strip()

    logger.debug(f'Получение путь до целевого тома uid={volume_to}.')
    try:
        volume_to_path = get_volume_path(db_connector, volume_to)
    except (DBConnectError, DBExecuteQueryError):
        logger.error(f'Не удалось выполнить запрос в БД. Ошибка: {err}')
        return
    if not volume_to_path:
        logger.error(f'Не удалось найти том с uid={volume_to}.')
        return
    volume_to_path = volume_to_path.strip()

    logger.info(f'Сканирование тома {volume_from_path}.')
    volume_from_dirs = scan_directory(
        volume_from_path,
        exclude_files=True,
        name_filter=lambda name: matches_date_pattern(name, move_older_days),
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

            logger.debug(f'Получение абсолютного пути для файла. '
                         f'volume_to_path={volume_to_path}, image_rel_path={image_rel_path}.')
            path_to = str(os.path.join(volume_to_path, image_rel_path))

            if not os.path.exists(path_from):
                logger.error(f'Файл {path_from} отсутствует.')
                continue

            logger.debug(f'Перенос файла {path_from} -> {path_to}.')

            try:
                copy_file(path_from, path_to)
                update_image(
                    db_connector, image_uid=image_uid, share_uid=volume_to, image_path=image_rel_path)
                logger.debug(f'Файл успешно скопирован: {path_from} -> {path_to}')
                num_copied_files += 1
                remove_file(path_from)
            except CopyFileError as err:
                logger.error(f'Не удалось скопировать файл: {path_from} -> {path_to}. Ошибка: {err}')
            except (DBConnectError, DBExecuteQueryError) as err:
                logger.error(f'Не удалось выполнить запрос в БД. Ошибка: {err}')
                try:
                    remove_file(path_to)
                except RemoveFileError as err:
                    logger.error(f'Не удалось удалить скопированный файл: {path_to}. Ошибка: {err}')
            except RemoveFileError as err:
                logger.error(f'Не удалось удалить изначальный файл: {path_from}. Ошибка: {err}')
                num_not_removed_files += 1

        logger.info(f'Скопировано всего/Скопировано успешно/Не удалось удалить/Всего: '
                    f'{num_copied_files}/{num_copied_files - num_not_removed_files}/'
                    f'{num_not_removed_files}/{num_all_files}. Директория: {v_dir.name}.')
