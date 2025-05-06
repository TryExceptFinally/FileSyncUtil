import logging
import os

from datetime import datetime
from time import sleep

from src.cases import move_images
from src.config import Config
from src.database import DatabaseConnector
from src.exceptions import ConfigError
from src.logger import configure_logging

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

if __name__ == '__main__':
    logger.info('Программа запущена.')

    try:
        _start_time = datetime.strptime(config.start_time, '%H:%M').time()
    except ValueError as e:
        logger.error(f'Не удалось получить время из: {config.start_time}. Нужный формат: %H:%M.')
        raise ConfigError()

    while True:
        if _start_time == datetime.now().time().replace(second=0, microsecond=0):
            logger.info('Запущен перенос файлов.')
            move_images(
                db_connector=db_connector,
                volume_from=config.volume_from,
                volume_to=config.volume_to,
                move_older_days=config.move_older_days,
            )
        sleep(60)
