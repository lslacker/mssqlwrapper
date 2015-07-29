__author__ = 'Lmai'

import pyodbc

class DB(object):
    _conn = None
    def __init__(self):
        pass

    @staticmethod
    def get_db(connection_string):
        pass


conn = pyodbc.connect(r'Driver={SQL Server Native Client 11.0};Server=10.3.8.41\WEBSQL;Database=lonsec;Trusted_Connection=yes;')