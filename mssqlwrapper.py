__author__ = 'Lmai'

import pyodbc


class DB:

    def __init__(self, connection_string):
        self._conn = pyodbc.connect(connection_string)
        self._cursor = self._conn.cursor()

    @classmethod
    def from_connection_string(cls, connection_string):
        db_instance = cls(connection_string)
        return db_instance

    def get_data(self, query, *argv):
        rows = self._cursor.execute(query, *argv).fetchall()
        return rows
