class DBExecuteQueryError(Exception):
    """Ошибка выполнения запроса"""


class RemoveFileError(Exception):
    """Ошибка удаления файла"""


class CopyFileError(Exception):
    """Ошибка копирования файла"""


class ConfigError(Exception):
    """Ошибка конфигурации"""
