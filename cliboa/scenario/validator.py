#
# Copyright BrainPad Inc. All Rights Reserved.
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
from cliboa.adapter.sqlite import SqliteAdapter
from cliboa.util.exception import InvalidParameter, SqliteInvalid
from cliboa.util.lisboa_log import LisboaLog


class EssentialParameters(object):
    """
    Validation for the essential parameters of step class
    """

    def __init__(self, cls_name, param_list):
        """
        Args:
            cls_name: class name which has validation target parameters
            param_list: list of validation target parameters
        """
        self._cls_name = cls_name
        self._param_list = param_list

    def __call__(self):
        for p in self._param_list:
            if not p:
                raise InvalidParameter(
                    "The essential parameter is not specified in %s." % self._cls_name
                )


class SqliteTableExistence(object):
    """
    Validation for the table of sqlite
    """

    def __init__(self, dbname, tblname, returns_bool=False):
        """
        Args:
            dbname: database name
            tblname: table name
            returns_bool: return bool or not
        """
        self._sqlite_adptr = SqliteAdapter()
        self._dbname = dbname
        self._tblname = tblname
        self._returns_bool = returns_bool
        self._logger = LisboaLog.get_logger(__name__)

    def __call__(self):
        try:
            self._sqlite_adptr.connect(self._dbname)
            cur = self._sqlite_adptr.fetch(
                'SELECT name FROM sqlite_master WHERE type="table" AND name="%s"' % self._tblname
            )
            result = cur.fetchall()
            if self._returns_bool is True:
                return True if result else False

            if not result and self._returns_bool is False:
                raise SqliteInvalid("Sqlite table %s not found" % self._tblname)

        finally:
            self._sqlite_adptr.close()
