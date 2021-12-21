from abc import abstractmethod

from cliboa.util.exception import DatabaseException
from cliboa.util.lisboa_log import LisboaLog


class RdbmsSupport:
    """
    This class allows you to access a database and
    provides database transaction by context manager.

    We highly recommended that you use this class via context manager,
    not creating instance itself.

    By accessing context manager, the class automatically connects a database,
    commit or rollback(when error occurred) and finally closed the connection.

    This class cannot be used by itself.
    Subclass must be created and implement abstract method that is to give a connection.
    (The way of accessing a database is depends on what kind of librairs you are using)

    """

    def __init__(self, host, user, password, dbname, port=None, encoding="UTF8"):
        self._logger = LisboaLog.get_logger(__name__)

        self._host = host
        self._user = user
        self._password = password
        self._dbname = dbname
        self._encoding = encoding
        self._port = port
        self._con = None

    def __enter__(self):
        """
        with RdbmsSupport open
        """
        self._begin()
        return self

    def __exit__(self, *exc):
        """
        with RdbmsSupport close
        """
        e_type, e_val, _ = exc
        try:
            if e_type:
                self._logger.error("Exception object: %s" % e_type)
                self._logger.error("Exception detail: %s" % e_val)
                self._rollback()
            else:
                self._commit()
        finally:
            self._end()

    def _begin(self):
        self._con = self.get_connection()
        self._logger.info(
            "Connected to database(host=%s, user=%s, db=%s)"
            % (self._host, self._user, self._dbname)
        )

    def _commit(self):
        if self._con:
            self._con.commit()
        else:
            raise DatabaseException("No database connection. Commit failed")

    def _rollback(self):
        if self._con:
            self._con.rollback()
        else:
            raise DatabaseException("No database connection. Rollback failed")

    def _end(self):
        if self._con:
            self._con.close()
            self._logger.info("Connection closed.")

    def execute(self, sql, params=None):
        """
        Execute a query and returns a result.
        This method will be executed connection.execute() without considering
        the differences between rdbms libraries.
        Therefor this can be executed any methods, but can NOT be garanteed an expected result.

        Args:
            sql (str): query to execute
            params=None (list): query parameters

        Returns:
            query result
        """
        with self._con.cursor() as cursor:
            if params:
                ret = cursor.execute(sql, params)
            else:
                ret = cursor.execute(sql)
        return ret

    def select(self, sql, params=None):
        cursor = self._con.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor

    def insert(self, sql, params=None):
        raise Exception("Must be implemented in a sub class")

    def update(self, sql, params=None):
        raise Exception("Must be implemented in a sub class")

    def delete(self, sql, params=None):
        raise Exception("Must be implemented in a sub class")

    @abstractmethod
    def get_connection(self, **kwargs):
        """
        Returns a database connection you want to access to
        """
