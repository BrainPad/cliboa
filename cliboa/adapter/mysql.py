import pymysql

from cliboa.adapter.rdbms import RdbmsSupport


class MysqlAdaptor(RdbmsSupport):
    def get_connection(self, **kwargs):
        # see https://pymysql.readthedocs.io/en/latest/modules/connections.html
        kwargs["host"] = self._host
        kwargs["user"] = self._user
        kwargs["password"] = self._password
        kwargs["db"] = self._dbname
        if self._port:
            kwargs["port"] = self._port
        return pymysql.connect(**kwargs)

    def insert(self, sql, params):
        self._con.cursor().executemany(sql, params)
