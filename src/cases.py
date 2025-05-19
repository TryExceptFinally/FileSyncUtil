import logging
import os

from src.database import DatabaseConnector
from src.dicom.exceptions import DicomError
from src.exceptions import CopyFileError, DBExecuteQueryError, RemoveFileError, DBConnectError, RenameFileError
from src.makstor.constants import MAKSTOR_UNREADABLE_PREFIX
from src.makstor.repository import MakstorRepository
from src.dicom.service import DicomService

from src.utils import (
    scan_directory, matches_date_pattern,
    extract_image_id_from_name, extract_rel_path_from_abs_path,
    copy_file, remove_file, rename_file, is_empty_dir,
)

logger = logging.getLogger(__name__)


def move_images(
    db_connector: DatabaseConnector,
    volume_from: int,
    volume_to: int,
    move_older_days: int,
    uid: int,
    gid: int,
    dir_not_found: str,
):
    makstor_repository = MakstorRepository(db_connector)

    logger.debug(f'Получение путь до тома источника uid={volume_from}.')
    try:
        volume_from_path = makstor_repository.get_volume_path(volume_from)
    except (DBConnectError, DBExecuteQueryError) as err:
        logger.error(f'Не удалось выполнить запрос в БД. Ошибка: {err}')
        return
    if not volume_from_path:
        logger.error(f'Не удалось найти том с uid={volume_from}.')
        return
    volume_from_path = volume_from_path.strip()

    logger.debug(f'Получение путь до целевого тома uid={volume_to}.')
    try:
        volume_to_path = makstor_repository.get_volume_path(volume_to)
    except (DBConnectError, DBExecuteQueryError) as err:
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
            image = None
            is_use_image_path_from_db = False

            logger.debug(f'Извлечение id из имени файла {d_file.name}.')
            image_id_from_file = extract_image_id_from_name(d_file.name)
            if image_id_from_file:
                logger.debug(f'Запрос image по id={image_id_from_file} из БД.')
                try:
                    image = makstor_repository.get_image_by_id(image_id_from_file)
                    if not image:
                        logger.debug('Не удалось получить image по id из БД.')
                except (DBConnectError, DBExecuteQueryError) as err:
                    logger.error(f'Не удалось выполнить запрос в БД. Ошибка: {err}')
            else:
                logger.debug('Не удалось извлечь id из имени файла.')

            if not image:
                try:
                    logger.debug(f'Извлечение uid из файла {d_file.name}.')
                    image_uid = DicomService(d_file.path).get_image_uid()
                    logger.debug(f'Запрос image по uid={image_uid}.')
                    image = makstor_repository.get_image_by_uid(image_uid)
                    if not image:
                        logger.error(f'Не удалось найти image {d_file.path} в БД.')

                        path_from = d_file.path
                        path_to = str(os.path.join(
                            dir_not_found,
                            d_file.name,
                        ))
                        # Использую префикс, для корректной работы авто-добавления из папки архивом
                        path_to_with_prefix = str(os.path.join(
                            dir_not_found,
                            f'{MAKSTOR_UNREADABLE_PREFIX}{d_file.name}',
                        ))
                        logger.debug(f'Перемещение ненайденного image '
                                     f'{path_from} -> {path_to_with_prefix}.')
                        try:
                            copy_file(
                                path_from=path_from, path_to=path_to_with_prefix, uid=uid, gid=gid)
                            rename_file(path_to_with_prefix, path_to)
                            remove_file(d_file.path)
                            logger.debug(f'Файл успешно перемещен: {path_from} -> {path_to}.')
                        except CopyFileError as err:
                            logger.error(f'Не удалось скопировать файл: '
                                         f'{path_from} -> {path_to_with_prefix}. '
                                         f'Ошибка: {err}')
                        except RenameFileError as err:
                            logger.error(f'Не удалось переименовать файл: '
                                         f'{path_to_with_prefix} -> {path_to}. '
                                         f'Ошибка: {err}')
                        except RemoveFileError as err:
                            logger.error(f'Не удалось удалить изначальный файл: {path_from}. '
                                         f'Ошибка: {err}')
                        continue

                    # В случае, если найден файл по uid использую отн. путь до файла из БД
                    # чтобы избежать дубликатов на целевом томе
                    is_use_image_path_from_db = True
                except DicomError as err:
                    logger.error(f'Не удалось получить uid из файла {d_file.name}. '
                                 f'Ошибка: {err}')
                except (DBConnectError, DBExecuteQueryError) as err:
                    logger.error(f'Не удалось выполнить запрос в БД. '
                                 f'Ошибка: {err}')

            image_id = image[0]

            if is_use_image_path_from_db:
                logger.debug(f'Используется путь до файла из БД.')
                image_rel_path = image[1]
            else:
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
                copy_file(
                    path_from=path_from, path_to=path_to, uid=uid, gid=gid)
                makstor_repository.update_image(
                    image_id=image_id, share_uid=volume_to, image_path=image_rel_path)
                num_copied_files += 1
                remove_file(path_from)
                logger.debug(f'Файл успешно перемещен: {path_from} -> {path_to}.')
            except CopyFileError as err:
                logger.error(f'Не удалось скопировать файл: {path_from} -> {path_to}. '
                             f'Ошибка: {err}')
            except (DBConnectError, DBExecuteQueryError) as err:
                logger.error(f'Не удалось выполнить запрос в БД. '
                             f'Ошибка: {err}')
                try:
                    remove_file(path_to)
                except RemoveFileError as err:
                    logger.error(f'Не удалось удалить скопированный файл: {path_to}. '
                                 f'Ошибка: {err}')
            except RemoveFileError as err:
                logger.error(f'Не удалось удалить изначальный файл: {path_from}. '
                             f'Ошибка: {err}')
                num_not_removed_files += 1

        if is_empty_dir(v_dir.path):
            try:
                remove_file(v_dir.path)
                logger.info(f'Директория {v_dir.path} удалена.')
            except RemoveFileError as err:
                logger.error(f'Не удалось удалить пустую директорию: {v_dir.path}. '
                             f'Ошибка: {err}')

        logger.info(f'Скопировано всего/Скопировано успешно/Не удалось удалить/Всего: '
                    f'{num_copied_files}/{num_copied_files - num_not_removed_files}/'
                    f'{num_not_removed_files}/{num_all_files}. Директория: {v_dir.name}.')
