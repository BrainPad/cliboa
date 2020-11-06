import pymysql

from cliboa.adapter.rdbms import RdbmsSupport


class MysqlAdaptor(RdbmsSupport):
    def get_connection(self, **kwargs):
        # see https://pymysql.readthedocs.io/en/latest/modules/connections.html
        kwargs["host"] = self._host
        kwargs["user"] = self._user
        kwargs["password"] = self._password
        kwargs["db"] = self._dbname
        return pymysql.connect(**kwargs)
