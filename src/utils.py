import logging
import os
import re
import shutil
import pwd
import grp

from datetime import datetime, timedelta
from typing import Callable

from src.exceptions import RemoveFileError, CopyFileError

logger = logging.getLogger(__name__)


def extract_rel_path_from_abs_path(base_path: str, abs_path: str) -> str:
    return abs_path.replace(base_path, '').lstrip('/')


def extract_image_id_from_name(name: str) -> int | None:
    pattern = r'\d+'

    uid = re.search(pattern, name)
    if not uid:
        return
    return int(uid.group())


def matches_date_pattern(name: str, older_days: int) -> bool:
    pattern = r'^\d{4}-(\d{2})-(\d{2})$'

    # Проверка формата даты
    if not re.match(pattern, name):
        return False

    # Проверка корректности даты
    dt = None
    try:
        dt = datetime.strptime(name, '%Y-%m-%d')
    except ValueError:
        pass

    if dt is None:
        return False

    if dt > datetime.now() - timedelta(days=older_days):
        return False

    return True


def scan_directory(
    path: str,
    exclude_files: bool = False,
    exclude_dirs: bool = False,
    name_filter: Callable[[str], bool] | None = None,
) -> list[os.DirEntry]:
    if exclude_dirs and exclude_files:
        return []
    result = []
    with os.scandir(path) as entries:
        for entry in entries:
            if entry.is_dir(follow_symlinks=False):
                if exclude_dirs:
                    continue
                if name_filter and not name_filter(entry.name):
                    continue
                result.append(entry)
            elif entry.is_file(follow_symlinks=False):
                if exclude_files:
                    continue
                if name_filter and not name_filter(entry.name):
                    continue
                result.append(entry)
    return result


def remove_file(path: str | bytes | os.PathLike[str] | os.PathLike[bytes]):
    try:
        os.remove(path)
    except OSError as e:
        logger.debug(f'Не удалось удалить файл: {path}. Ошибка: {e}')
        raise RemoveFileError(e)


def copy_file(path_from: str, path_to: str, uid: int, gid: int):
    """Копирует файл в папку (создает если нет) и устанавливает владельца и группу"""
    path_to_dir = os.path.dirname(path_to)
    try:
        if not os.path.exists(path_to_dir) or not os.path.isdir(path_to_dir):
            os.makedirs(path_to_dir, exist_ok=True)
            os.chown(path_to_dir, uid, gid)
        shutil.copy2(path_from, path_to)
        os.chown(path_to, uid, gid)
    except Exception as e:
        logger.debug(f'Не удалось скопировать файл: {path_from} -> {path_to}. Ошибка: {e}')
        raise CopyFileError(e)


def get_uid_gid(owner_name: str, group_name: str) -> tuple[int | None, int | None]:
    """Получает UID и GID с помощью имени владельца и группы"""
    try:
        uid = pwd.getpwnam(owner_name).pw_uid
        gid = grp.getgrnam(group_name).gr_gid
        return uid, gid
    except KeyError as e:
        logger.debug(f'Не удалось получить UID и GID для {owner_name} и {group_name}. Ошибка: {e}')
        return None, None
