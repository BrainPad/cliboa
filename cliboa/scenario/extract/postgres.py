import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2.extras
import ast
import codecs
import csv
from cliboa.scenario.base import BasePostgres
from cliboa.scenario.validator import (EssentialParameters, IOOutput,
                                       PostgresTableExistence)
from cliboa.util.exception import FileNotFound, PostgresInvalid


class PostgresRead(BasePostgres):

    def __init__(self):
        super().__init__()
        """
        Implement attributes to be set in scenario.yml
        """
        self._tblname = None
        self._raw_query = None

    def tblname(self, tblname):
        self._tblname = tblname

    def raw_query(self, raw_query):
        self._raw_query = raw_query

    def execute(self, *args):
        super().execute()
        
        """
        Implement processes which would like to do
        """
        input_valid = IOInput(self._io)
        input_valid()

        param_valid = EssentialParameters(self.__class__.__name__, [self._tblname,self._password,self._host,self._dbname,self._password])
        param_valid()
        
        tbl_valid = PostgresTableExistence(self._host, self._user, 
            self._dbname, self._tblname, self._password)
        tbl_valid()
      
        def dict_factory(cursor, row):
            d = {}
            for i, col in enumerate(cursor.description):
                d[col[0]] = row[i]
            return d
      
        def dict_f(row):
            d = {}
            for i, col in enumerate(colnames):
                d[col[0]] = row[i]
                print(row[i])
            return d

        

        conn = self._postgres_adptr.connect(self._host, self._user,
            self._dbname, self._password)
        

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor) 
           
        cur.execute(self.__get_query())
        results = cur.fetchall()
         
        dict_result = []
        for row in results:
            dict_result.append(dict(row))

        
        for r in dict_result:
            self._s.save(r)
   
    def __get_query(self):
        """
        Get sql to read
        """
        if self._raw_query:
            return self._raw_query
        sql = ""
        if self._columns:
            select_columns = ",".join(map(str, self._columns))
            sql = "SELECT %s FROM %s" % (select_columns, self._tblname)
        else:
            sql = "SELECT * FROM %s" % self._tblname
        return sql

class PostgresReadRow(BasePostgres):
    """
    Execute query.
    """

    def __init__(self):
        super().__init__()

    def execute(self, *args):
        super().execute()

        self._postgres_adptr.connect(self._host, self._user, self._dbname, self._password)
        try:
            cur = self._postgres_adptr.fetch(
                sql=self._get_query(), row_factory=self._get_factory()
            )
            self._callback_handler(cur)
        finally:
            self._postgres_adptr.close()

    def _get_factory(self):
        """
        Default row factory (returns value as tuple) is used if factory is not set
        """
        return None

    def _get_query(self):
        raise NotImplementedError("Method 'get_query' must be implemented by subclass")

    def _callback_handler(self, cursor):
        raise NotImplementedError(
            "Method 'callback_handler' must be implemented by subclass"
        )
