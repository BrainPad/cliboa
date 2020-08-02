#
# Copyright 2019 BrainPad Inc. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
from cliboa.scenario.base import BaseSqlite
from cliboa.scenario.validator import EssentialParameters, IOInput, SqliteTableExistence


# deprecated
class SqliteRead(BaseSqlite):
    """
    Select * from the specifed table
    """

    def __init__(self):
        super().__init__()
        self._tblname = None
        self._raw_query = None

    def tblname(self, tblname):
        self._tblname = tblname

    def raw_query(self, raw_query):
        self._raw_query = raw_query

    def execute(self, *args):
        super().execute()

        input_valid = IOInput(self._io)
        input_valid()

        param_valid = EssentialParameters(self.__class__.__name__, [self._tblname])
        param_valid()

        tbl_valid = SqliteTableExistence(self._dbname, self._tblname)
        tbl_valid()

        def dict_factory(cursor, row):
            d = {}
            for i, col in enumerate(cursor.description):
                d[col[0]] = row[i]
            return d

        self._sqlite_adptr.connect(self._dbname)
        cur = self._sqlite_adptr.fetch(sql=self.__get_query(), row_factory=dict_factory)
        for r in cur:
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


class SqliteReadRow(BaseSqlite):
    """
    Execute query.
    """

    def __init__(self):
        super().__init__()

    def execute(self, *args):
        super().execute()

        self._sqlite_adptr.connect(self._dbname)
        try:
            cur = self._sqlite_adptr.fetch(
                sql=self._get_query(), row_factory=self._get_factory()
            )
            self._callback_handler(cur)
        finally:
            self._sqlite_adptr.close()

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
