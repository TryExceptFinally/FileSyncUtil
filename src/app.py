import logging
import os
from enum import IntEnum

from src.database import DatabaseConnector
from src.dicom.exceptions import DicomError
from src.dicom.service import DicomService
from src.exceptions import RemoveDirError, DBConnectError, DBExecuteQueryError, CopyFileError, RenameFileError, \
    RemoveFileError
from src.makstor.constants import MAKSTOR_UNREADABLE_PREFIX
from src.makstor.repository import MakstorRepository
from src.utils import remove_dir, scan_directory, matches_date_pattern, extract_image_id_from_name, copy_file, \
    rename_file, remove_file, extract_rel_path_from_abs_path, is_empty_dir

logger = logging.getLogger(__name__)


class MoveFileStatus(IntEnum):
    MOVED = 0
    SKIPPED = 1
    ONLY_COPIED = 2
    NOT_FOUND_MOVED = 3
    NOT_FOUND_SKIPPED = 4
    NOT_FOUND_ONLY_COPIED = 5
    NOT_FOUND_ONLY_COPIED_AND_RENAMED = 6


class FileSyncApp:
    def __init__(
        self,
        db_connector: DatabaseConnector,
        volume_from: int,
        volume_to: int,
        move_older_days: int,
        dir_not_found: str,
        uid: int,
        gid: int,
        is_volume_to_network: bool,
        is_dir_not_found_network: bool,
    ):
        self.volume_from = volume_from
        self.volume_from_path = ''
        self.volume_to = volume_to
        self.volume_to_path = ''
        self.move_older_days = move_older_days
        self.dir_not_found = dir_not_found
        self.uid = uid
        self.gid = gid
        self.is_volume_to_network = is_volume_to_network
        self.is_dir_not_found_network = is_dir_not_found_network
        # repositories
        self.makstor_repository = MakstorRepository(db_connector)

    @staticmethod
    def _remove_dir(path: str):
        try:
            remove_dir(path)
            logger.info(f'Директория {path} удалена.')
        except RemoveDirError as err:
            logger.error(f'Не удалось удалить пустую директорию: {path}. '
                         f'Ошибка: {err}')

    def _get_volume_path(self, volume_id: int) -> str | None:
        try:
            volume_path = self.makstor_repository.get_volume_path(volume_id)
        except (DBConnectError, DBExecuteQueryError) as err:
            logger.error(f'Не удалось выполнить запрос в БД. Ошибка: {err}')
            return
        if not volume_path:
            return
        return volume_path.strip()

    def _scan_volume(self, volume_path: str) -> list[os.DirEntry]:
        volume_dirs = scan_directory(
            volume_path,
            exclude_files=True,
            name_filter=lambda name: matches_date_pattern(name, self.move_older_days),
        )
        return volume_dirs

    def _move_file(self, file: os.DirEntry) -> MoveFileStatus:
        image = None
        is_use_image_path_from_db = False

        logger.debug(f'Извлечение id из имени файла {file.name}.')
        image_id_from_file = extract_image_id_from_name(file.name)
        if image_id_from_file:
            logger.debug(f'Запрос image по id={image_id_from_file} из БД.')
            try:
                image = self.makstor_repository.get_image_by_id(image_id_from_file)
                if not image:
                    logger.debug('Не удалось получить image по id из БД.')
            except (DBConnectError, DBExecuteQueryError) as err:
                logger.error(f'Не удалось выполнить запрос в БД. '
                             f'Ошибка: {err}')
                return MoveFileStatus.SKIPPED
        else:
            logger.debug('Не удалось извлечь id из имени файла.')

        if not image:
            logger.debug(f'Извлечение uid из файла {file.name}.')
            try:
                image_uid = DicomService(file.path).get_image_uid()
                logger.debug(f'Запрос image по uid={image_uid}.')
                image = self.makstor_repository.get_image_by_uid(image_uid)
            except DicomError as err:
                logger.error(f'Не удалось получить uid из файла {file.name}. '
                             f'Ошибка: {err}')
            except (DBConnectError, DBExecuteQueryError) as err:
                logger.error(f'Не удалось выполнить запрос в БД. '
                             f'Ошибка: {err}')

            if not image:
                logger.error(f'Не удалось найти image {file.path} в БД.')
                # В случае, если найден файл по uid использую отн. путь до файла из БД
                # чтобы избежать дубликатов на целевом томе
                is_use_image_path_from_db = True

                path_from = file.path
                path_to = str(os.path.join(
                    self.dir_not_found,
                    file.name,
                ))
                # Использую префикс, для корректной работы авто-добавления из папки архивом
                path_to_with_prefix = str(os.path.join(
                    self.dir_not_found,
                    f'{MAKSTOR_UNREADABLE_PREFIX}{file.name}',
                ))
                logger.debug(f'Перемещение ненайденного image '
                             f'{path_from} -> {path_to_with_prefix}.')
                try:
                    if self.is_dir_not_found_network:
                        copy_file(
                            path_from=path_from,
                            path_to=path_to_with_prefix,
                        )
                    else:
                        copy_file(
                            path_from=path_from,
                            path_to=path_to_with_prefix,
                            uid=self.uid,
                            gid=self.gid,
                        )
                    rename_file(path_to_with_prefix, path_to)
                    remove_file(file.path)
                    logger.debug(f'Файл успешно перемещен: {path_from} -> {path_to}.')
                    return MoveFileStatus.NOT_FOUND_MOVED
                except CopyFileError as err:
                    logger.error(f'Не удалось скопировать файл: '
                                 f'{path_from} -> {path_to_with_prefix}. '
                                 f'Ошибка: {err}')
                    return MoveFileStatus.NOT_FOUND_SKIPPED
                except RenameFileError as err:
                    logger.error(f'Не удалось переименовать файл: '
                                 f'{path_to_with_prefix} -> {path_to}. '
                                 f'Ошибка: {err}')
                    return MoveFileStatus.NOT_FOUND_ONLY_COPIED
                except RemoveFileError as err:
                    logger.error(f'Не удалось удалить изначальный файл: {path_from}. '
                                 f'Ошибка: {err}')
                    return MoveFileStatus.NOT_FOUND_ONLY_COPIED_AND_RENAMED

        image_id = image[0]

        if is_use_image_path_from_db:
            logger.debug(f'Используется путь до файла из БД.')
            image_rel_path = image[1]
        else:
            logger.debug(f'Извлечение относительного пути из абсолютного. '
                         f'base_path={self.volume_from_path}, abs_path={file.path}.')
            image_rel_path = extract_rel_path_from_abs_path(
                base_path=self.volume_from_path, abs_path=file.path)

        path_from = file.path

        logger.debug(f'Получение абсолютного пути для файла. '
                     f'volume_to_path={self.volume_to_path}, image_rel_path={image_rel_path}.')
        path_to = str(os.path.join(self.volume_to_path, image_rel_path))

        if not os.path.exists(path_from):
            logger.error(f'Файл {path_from} отсутствует.')
            return MoveFileStatus.SKIPPED

        logger.debug(f'Перенос файла {path_from} -> {path_to}.')

        try:
            if self.is_volume_to_network:
                copy_file(
                    path_from=path_from,
                    path_to=path_to,
                )
            else:
                copy_file(
                    path_from=path_from,
                    path_to=path_to,
                    uid=self.uid,
                    gid=self.gid,
                )
            self.makstor_repository.update_image(
                image_id=image_id, share_uid=self.volume_to, image_path=image_rel_path)
            remove_file(path_from)
            logger.debug(f'Файл успешно перемещен: {path_from} -> {path_to}.')
            return MoveFileStatus.MOVED
        except CopyFileError as err:
            logger.error(f'Не удалось скопировать файл: {path_from} -> {path_to}. '
                         f'Ошибка: {err}')
            return MoveFileStatus.SKIPPED
        except (DBConnectError, DBExecuteQueryError) as err:
            logger.error(f'Не удалось выполнить запрос в БД. '
                         f'Ошибка: {err}')
            try:
                remove_file(path_to)
                return MoveFileStatus.SKIPPED
            except RemoveFileError as err:
                logger.error(f'Не удалось удалить скопированный файл: {path_to}. '
                             f'Ошибка: {err}')
                return MoveFileStatus.ONLY_COPIED
        except RemoveFileError as err:
            logger.error(f'Не удалось удалить изначальный файл: {path_from}. '
                         f'Ошибка: {err}')
            return MoveFileStatus.ONLY_COPIED

    def run(self):
        logger.debug(f'Получение путь до тома источника uid={self.volume_from}.')
        self.volume_from_path = self._get_volume_path(self.volume_from)
        if not self.volume_from_path:
            logger.error(f'Не удалось найти том источника с uid={self.volume_from}.')
            return

        logger.debug(f'Получение путь до целевого тома uid={self.volume_to}.')
        self.volume_to_path = self._get_volume_path(self.volume_to)
        if not self.volume_to_path:
            logger.error(f'Не удалось найти целевой том с uid={self.volume_to}.')
            return

        logger.info(f'Сканирование тома источника {self.volume_from_path}.')
        volume_from_dirs = self._scan_volume(self.volume_from_path)
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
                logger.info('Не найдено файлов для переноса. Удаление директории...')
                self._remove_dir(v_dir.path)
                continue

            num_all_files = len(dir_files)

            logger.info(f'Найдено {num_all_files} файлов.')

            num_moved_files = 0
            num_skipped_files = 0
            num_only_copied_files = 0
            num_not_found_moved_files = 0
            num_not_found_skipped_files = 0
            num_not_found_only_copied_files = 0
            num_not_found_only_copied_and_renamed_files = 0

            for d_file in dir_files:
                moved_status = self._move_file(d_file)
                match moved_status:
                    # Base
                    case MoveFileStatus.MOVED:
                        num_moved_files += 1
                    case MoveFileStatus.SKIPPED:
                        num_skipped_files += 1
                    case MoveFileStatus.ONLY_COPIED:
                        num_only_copied_files += 1
                    # Not found
                    case MoveFileStatus.NOT_FOUND_MOVED:
                        num_not_found_moved_files += 1
                    case MoveFileStatus.NOT_FOUND_SKIPPED:
                        num_not_found_skipped_files += 1
                    case MoveFileStatus.NOT_FOUND_ONLY_COPIED:
                        num_not_found_only_copied_files += 1
                    case MoveFileStatus.NOT_FOUND_ONLY_COPIED_AND_RENAMED:
                        num_not_found_only_copied_and_renamed_files += 1

            # Если после переноса папка осталось пустой - удаляю
            if is_empty_dir(v_dir.path):
                self._remove_dir(v_dir.path)

            logger.info(
                f'Всего файлов: {num_all_files} в директории {v_dir.name}. Из них:\n'
                f'Перемещено: {num_moved_files}\n'
                f'Только скопировано: {num_only_copied_files}\n'
                f'Пропущено: {num_skipped_files}\n'
                f'Для ненайденных файлов в БД:\n'
                f'Перемещено: {num_not_found_moved_files}\n'
                f'Только скопировано: {num_not_found_only_copied_files}\n'
                f'Только скопировано и переименовано: {num_not_found_only_copied_and_renamed_files}\n'
                f'Пропущено: {num_not_found_skipped_files}\n'
            )
