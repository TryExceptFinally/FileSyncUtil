class DBConnectError(Exception):
    """Ошибка подключения к БД"""


class DBExecuteQueryError(Exception):
    """Ошибка выполнения запроса"""


class RemoveFileError(Exception):
    """Ошибка удаления файла"""


class RemoveDirError(Exception):
    """Ошибка удаления директории"""


class RenameFileError(Exception):
    """Ошибка переименования файла"""


class CopyFileError(Exception):
    """Ошибка копирования файла"""


class ConfigError(Exception):
    """Ошибка конфигурации"""
