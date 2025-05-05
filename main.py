import logging
import os
import shutil
from datetime import datetime, timedelta

from src.config import Config
from src.database import DatabaseConnector
from src.exceptions import DBExecuteQueryError, RemoveFileError, CopyFileError
from src.logger import configure_logging
from src.utils import remove_file, copy_file

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

if __name__ == '__main__':
    with db_connector as db:
        volume_from_path = db.execute(
            f"""select share_path 
            from shares 
            where share_uid='{config.volume_from}'"""
        ).fetchone()[0].strip()
        volume_to_path = db.execute(
            f"""select share_path 
            from shares 
            where share_uid='{config.volume_to}'"""
        ).fetchone()[0].strip()

        max_date = db.execute(
            f"""select study_addition_datetime 
            from studies 
            order by study_addition_datetime desc 
            limit 1"""
        ).fetchone()[0]
        min_date = db.execute(
            f"""select study_addition_datetime 
            from studies 
            order by study_addition_datetime asc 
            limit 1"""
        ).fetchone()[0]

        end_date = datetime.now() - timedelta(days=0)
        start_date = min_date

        if start_date > end_date:
            print('Нет исследований для перемещения')
            exit()

        delta = end_date - start_date

        for i in range(delta.days + 1):
            date = start_date + timedelta(days=i)
            date_str = date.strftime('%Y-%m-%d')
            print('Копирую дату:', date_str)
            images = db.execute(
                f"""select image_path 
                from images 
                where share_uid='{config.volume_from}' 
                and image_path like '{date_str}%'"""
            ).fetchall()
            for image in images:
                image_path = image[0].strip()
                path_from = str(os.path.join(volume_from_path, image_path))
                path_to = str(os.path.join(volume_to_path, image_path))

                if not os.path.exists(path_from):
                    logger.error(f'Файл отсутствует: {path_from}')
                    continue

                if os.path.exists(path_to):
                    logger.warning(f'Копируемый файл уже существует: {path_to}')

                try:
                    copy_file(path_from, path_to)
                    db.execute(
                        f"""update images 
                        set share_uid='{config.volume_to}' 
                        where image_path='{image_path}' 
                        and share_uid='{config.volume_from}'"""
                    )
                    logger.info(f'Файл успешно перемещен: {path_from} -> {path_to}')

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
