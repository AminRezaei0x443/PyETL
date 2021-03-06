from typing import Dict, Any

import psycopg2

from db.db_accessor import DatabaseAccessor


class PostgresAccessor(DatabaseAccessor):
    conn = None
    connected = False
    cursor_ = None
    accessors_cache: Dict[str, 'PostgresAccessor'] = {}

    @staticmethod
    def obtain(db="postgres", **kwargs) -> 'PostgresAccessor':
        if db in PostgresAccessor.accessors_cache:
            return PostgresAccessor.accessors_cache[db]
        pa = PostgresAccessor(**{"db": db, **kwargs})
        PostgresAccessor.accessors_cache[db] = pa
        return pa

    def __init__(self, host: str = "localhost",
                 port: int = 5432,
                 db: str = "postgres",
                 user: str = "",
                 pwd: str = "",
                 lazy: bool = False):
        super().__init__()
        self.host = host
        self.port = port
        self.db_name = db
        self.user = user
        self.pwd = pwd
        if not lazy:
            self.connect()
        pass

    def connect(self):
        self.conn = psycopg2.connect(host=self.host,
                                     port=self.port,
                                     dbname=self.db_name,
                                     user=self.user,
                                     password=self.pwd)
        self.connected = True

    def cursor(self, reuse=True):
        if not self.connected:
            self.connect()
        if reuse and self.cursor_ is not None:
            return self.cursor_
        self.cursor_ = self.conn.cursor()
        return self.cursor_

    def exec(self, query: str, q_vars=None, reuse_cursor=True):
        c = self.cursor(reuse=reuse_cursor)
        c.execute(query, q_vars)

    def fetch_all(self):
        if self.cursor_ is None:
            raise RuntimeError("No Active Cursor! Must've executed a cursor before requiring to fetch")
        return self.cursor_.fetchall()

    def fetch_one(self):
        if self.cursor_ is None:
            raise RuntimeError("No Active Cursor! Must've executed a cursor before requiring to fetch")
        return self.cursor_.fetchone()

    def commit(self):
        if not self.connected:
            raise RuntimeError("No Active Connection! Must've connected to a database instance before commit")
        self.conn.commit()
