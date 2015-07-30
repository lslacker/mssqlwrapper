__author__ = 'Lmai'

import pyodbc
import logging

'''
Would like to log the query like psycopg2 modgrify
http://stackoverflow.com/questions/5266430/how-to-see-the-real-sql-query-in-python-cursor-execute
'''


class DB:

    def __init__(self, connection_string):
        self._conn = pyodbc.connect(connection_string)
        self._cursor = self._conn.cursor()
        self.debug = False

    @classmethod
    def from_connection_string(cls, connection_string):
        db_instance = cls(connection_string)
        return db_instance

    @staticmethod
    def check_sql_string(sql, values):
        unique = "%PARAMETER%"
        sql = sql.replace("?", unique)
        for v in values:
            sql = sql.replace(unique, repr(v), 1)
        return '\n'+sql

    def get_data(self, query, *argv):
        if self.debug:
            logging.debug(self.check_sql_string(query, argv))
        rows = self._cursor.execute(query, *argv).fetchall()

        return rows


class TempTable:

    @staticmethod
    def from_data(db_instance, data, field_names):
        pass