import psycopg2
from psycopg2.extras import execute_values

from cliboa.adapter.rdbms import RdbmsSupport


class PostgresqlAdaptor(RdbmsSupport):
    def get_connection(self, **kwargs):
        # see https://www.psycopg.org/docs/
        kwargs["host"] = self._host
        kwargs["user"] = self._user
        kwargs["password"] = self._password
        kwargs["database"] = self._dbname
        if self._port:
            kwargs["port"] = self._port
        return psycopg2.connect(**kwargs)

    def insert(self, sql, params):
        # psycopg2.executemany() seems like bulk insert, but it's extremely slow.
        # Instead of useing it, it changes to execute_values method, but to do so
        # the query needs to be changed like below.
        # insert into tbl_name (id, name, memo) values (%s, %s, %s)
        # ↓
        # insert into tbl_name (id, name, memo) values %s
        pos = sql.rfind("VALUES")
        sql = sql[:pos]
        sql += " VALUES %s"
        execute_values(self._con.cursor(), sql, params)
