__author__ = 'Lmai'

import pyodbc
import logging
import uuid
import itertools
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
        rows = values
        if rows:
            if not all(isinstance(e, (list, tuple)) for e in values):   # debug executemany
                rows = [values]
            a = [[sql.replace(unique, repr(v), 1) for v in row] for row in rows]
            a = itertools.chain(*a)
            return '\n'+'\n'.join(a)
        return None

    def get_one_value(self, query, *argv):
        if self.debug:
            logging.debug(self.check_sql_string(query, argv))
        row = self._cursor.execute(query, *argv).fetchone()
        return row[0]

    def get_data(self, query, *argv):
        if self.debug:
            logging.debug(self.check_sql_string(query, argv))
        rows = self._cursor.execute(query, *argv).fetchall()
        return rows

    def execute(self, query, *argv):
        if self.debug:
            logging.debug(self.check_sql_string(query, argv))

        self._cursor.execute(query)
        return self._cursor.rowcount

    def executemany(self, query, list_of_tuple):
        if self.debug:
            logging.debug(self.check_sql_string(query, list_of_tuple))
        self._cursor.executemany(query, list_of_tuple)
        return self._cursor.rowcount

    def sp_columns(self, table_name, catalog=None, schema=None, column=None):
        logging.debug('lan=%s' % table_name)
        return [row.column_name for row in self._cursor.columns(table_name)]


class TempTable:

    @staticmethod
    def create_from_data(db_instance, data, create_qry):
        table_name = '#tmp_' + uuid.uuid4().get_hex().upper()[:16]
        db_instance.execute(create_qry.format(table_name=table_name))
        rows = db_instance.sp_columns(table_name+'%')
        print(rows)
        no_of_fields = db_instance.get_one_value('''\
        SELECT count(*)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME like ?
        ''', table_name+'%')

        db_instance.executemany('insert into {table_name} values({fields})' \
                                .format(table_name=table_name,
                                        fields=','.join(['?']*no_of_fields)), data)
        return table_name

