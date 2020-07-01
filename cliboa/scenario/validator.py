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

from cliboa.util.exception import InvalidParameter, ScenarioFileInvalid, SqliteInvalid
from cliboa.util.lisboa_log import LisboaLog
from cliboa.util.sqlite import SqliteAdapter


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
        self.__cls_name = cls_name
        self.__param_list = param_list

    def __call__(self):
        for p in self.__param_list:
            if not p:
                raise InvalidParameter(
                    "The essential parameter is not specified in %s." % self.__cls_name
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
        self.__sqlite_adptr = SqliteAdapter()
        self.__dbname = dbname
        self.__tblname = tblname
        self.__returns_bool = returns_bool
        self._logger = LisboaLog.get_logger(__name__)

    def __call__(self):
        try:
            self.__sqlite_adptr.connect(self.__dbname)
            cur = self.__sqlite_adptr.fetch(
                'SELECT name FROM sqlite_master WHERE type="table" AND name="%s"'
                % self.__tblname
            )
            result = cur.fetchall()
            if self.__returns_bool is True:
                return True if result else False

            if not result and self.__returns_bool is False:
                raise SqliteInvalid("Sqlite table %s not found" % self.__tblname)

        finally:
            self.__sqlite_adptr.close()


class IOInput(object):
    """
    Validation for io: input
    """

    def __init__(self, io):
        self.__io = io

    def __call__(self):
        if self.__io != "input":
            raise ScenarioFileInvalid("io: input is not specified.")


class IOOutput(object):
    """
    Validation for io: input
    """

    def __init__(self, io):
        self.__io = io

    def __call__(self):
        if self.__io != "output":
            raise ScenarioFileInvalid("io: output is not specified.")
