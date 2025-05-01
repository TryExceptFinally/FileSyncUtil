import logging
import os
import shutil

from src.exceptions import RemoveFileError, CopyFileError

logger = logging.getLogger(__name__)


def remove_file(path: str | bytes | os.PathLike[str] | os.PathLike[bytes]):
    try:
        os.remove(path)
    except OSError as e:
        logger.debug(f'Не удалось удалить файл: {path}. Ошибка: {e}')
        raise RemoveFileError(e)


def copy_file(path_from: str, path_to: str):
    try:
        os.makedirs(os.path.dirname(path_to), exist_ok=True)
        shutil.copy2(str(path_from), str(path_to))
    except Exception as e:
        logger.debug(f'Не удалось скопировать файл: {path_from} -> {path_to}. Ошибка: {e}')
        raise CopyFileError(e)
