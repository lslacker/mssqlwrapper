__author__ = 'Lmai'

import pyodbc
import logging
import uuid
import itertools
import re

'''
Would like to log the query like psycopg2 modgrify
http://stackoverflow.com/questions/5266430/how-to-see-the-real-sql-query-in-python-cursor-execute
'''

logger = logging.getLogger(__name__)


class DB:

    one_value = False

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
            if not all(isinstance(e, (list, tuple)) for e in values):  # debug executemany
                rows = [values]
            a = [[sql.replace(unique, repr(v), 1) for v in row] for row in rows]
            a = itertools.chain(*a)
            return '\n' + '\n'.join(a)
        return None

    def get_one_value(self, query, *argv):
        if self.debug:
            logger.debug(self.check_sql_string(query, argv))
        row = self._cursor.execute(query, *argv).fetchone()
        return row[0] if row else None

    def get_data(self, query, *argv):
        if self.debug:
            logger.debug(self.check_sql_string(query, argv))
        return self._cursor.execute(query, *argv).fetchall()

    def __iter__(self):
        return self

    def __next__(self):
        if self._cursor.nextset():
            try:
                # select query
                rows = self._cursor.fetchall()
            except pyodbc.ProgrammingError:
                # update, delete, insert query
                rows = self._cursor.rowcount
            return rows
        else:
            raise StopIteration

    def execute(self, query, *argv):
        if self.debug:
            logger.debug(self.check_sql_string(query, argv))

        self._cursor.execute(query)

        return self._cursor.rowcount

    def executemany(self, query, list_of_tuple):
        if not isinstance(list_of_tuple, list):
            list_of_tuple = list(list_of_tuple)
        if self.debug:
            logger.debug(self.check_sql_string(query, list_of_tuple))
        self._cursor.executemany(query, list_of_tuple)
        return self._cursor.rowcount

    def sp_columns(self, table_name):
        catalog = None
        schema = None
        try:
            catalog, schema, table_name = table_name.split('.')
            schema = schema or 'dbo'
        except ValueError:
            pass

        if '#' in table_name:
            query = '''
            select c.name as column_name from tempdb.sys.columns c
            inner join tempdb.sys.tables t ON c.object_id = t.object_id
            where t.name like ?
            '''
            self._cursor.execute(query, table_name + '%')
            return [row.column_name for row in self._cursor.fetchall()]
        else:
            try:
                # WIERD: if catalog and schema is none, we can't not pass it
                return [row.column_name for row in self._cursor.columns(table_name, catalog, schema)]
            except TypeError:
                return [row.column_name for row in self._cursor.columns(table_name)]

    def get_count_from(self, table_name):
        return self.get_one_value('select count(*) from {}'.format(table_name))

    def commit(self):
        self._cursor.commit()

    def close(self):
        self._cursor.close()
        self._conn.close()


class TempTable:
    def __init__(self, tt_name, qty):
        self.tt_name = tt_name
        self.qty = qty

    def __str__(self):
        return self.tt_name

    def __len__(self):
        return self.qty

    @staticmethod
    def get_tt_name():
        return '#tmp_' + uuid.uuid4().hex.upper()[:16]

    @classmethod
    def create_from_data(cls, db_instance, data, create_qry):
        table_name = TempTable.get_tt_name()
        db_instance.execute(create_qry.format(table_name=table_name))
        fields = db_instance.sp_columns(table_name + '%')
        no_of_fields = len(fields)
        db_instance.executemany('insert into {table_name} values({fields})'
                                .format(table_name=table_name,
                                        fields=','.join(['?'] * no_of_fields)), data)
        qty = db_instance.get_count_from(table_name)

        return cls(table_name, qty)

    @classmethod
    def create_from_query(cls, db_instance, qry):
        table_name = TempTable.get_tt_name()

        qry = re.sub(r'(?:\s+|\n)from', ' into {} from'.format(table_name), qry, count=1, flags=re.IGNORECASE | re.MULTILINE)

        db_instance.execute(qry)

        qty = db_instance.get_count_from(table_name)

        return cls(table_name, qty)
