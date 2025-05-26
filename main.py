import logging
import os

from datetime import datetime
from time import sleep

from src.app import FileSyncApp
from src.config import Config
from src.database import DatabaseConnector
from src.logger import configure_logging
from src.utils import get_uid_gid

main_path = os.path.dirname(__file__)

# Считываю конфиг
config = Config('config.ini').read()

# Конфигурирую логгинг
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

    while True:
        if config.start_time == datetime.now().time().replace(second=0, microsecond=0):
            uid, gid = get_uid_gid(config.owner_name, config.group_name)
            logger.info(f'Запущен перенос файлов '
                        f'UID:{config.owner_name}:{uid}, GID:{config.group_name}:{gid}.')
            FileSyncApp(
                db_connector=db_connector,
                volume_from=config.volume_from,
                volume_to=config.volume_to,
                move_older_days=config.move_older_days,
                uid=uid,
                gid=gid,
                dir_not_found=config.dir_not_found,
                is_volume_to_network=config.is_volume_to_network,
                is_dir_not_found_network=config.is_dir_not_found_network,
            ).run()
        sleep(60)
