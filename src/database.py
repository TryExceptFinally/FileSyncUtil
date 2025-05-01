import logging

import psycopg2
from psycopg2._psycopg import cursor

from src.exceptions import DBExecuteQueryError

logger = logging.getLogger(__name__)


class DatabaseConnector:
    def __init__(self, dbname: str, user: str, password: str, host: str, port: int):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

        self.conn = None
        self.cursor = None

    def __enter__(self):
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.cursor = self.conn.cursor()
        return self

    def execute(self, query: str, params=None) -> cursor | None:
        try:
            self.cursor.execute(query, params)
            if query.strip().lower().startswith('select'):
                return self.cursor
            else:
                self.conn.commit()
        except Exception as e:
            logger.debug(f'Error executing query: {query} with params: {params}. Exception: {e}')
            self.conn.rollback()
            raise DBExecuteQueryError(e)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
