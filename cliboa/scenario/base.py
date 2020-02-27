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
from abc import abstractmethod
from google.oauth2 import service_account
import os

from cliboa.scenario.validator import (
    EssentialParameters,
    SqliteTableExistence,
    IOOutput,
)
from cliboa.util.cache import StorageIO, StepArgument
from cliboa.util.file import File
from cliboa.util.sqlite import SqliteAdapter
from cliboa.util.exception import *


class BaseStep(object):
    """
    Base class of all the step classes
    """

    def __init__(self):
        self._s = StorageIO()
        self._step = None
        self._symbol = None
        self._parallel = None
        self._io = None
        self._logger = None

    @property
    def step(self):
        return self._step

    @step.setter
    def step(self, step):
        self._step = step

    @property
    def symbol(self):
        return self._symbol

    @symbol.setter
    def symbol(self, symbol):
        self._symbol = symbol

    @property
    def parallel(self):
        return self._parallel

    @parallel.setter
    def parallel(self, parallel):
        self._parallel = parallel

    @property
    def io(self):
        return self._io

    @io.setter
    def io(self, io):
        self._io = io

    @property
    def logger(self):
        return self._logger

    @logger.setter
    def logger(self, logger):
        self._logger = logger

    def trigger(self, *args):
        try:
            return self.execute(args)
        except Exception as e:
            return self._exception_dispatcher(e)

    @abstractmethod
    def execute(self, *args):
        pass

    def get_target_files(self, src_dir, src_pattern):
        """
        Search files either with regular expression
        """
        return File().get_target_files(src_dir, src_pattern)

    def get_step_argument(self, name):
        """
        Returns an argument from scenario.yaml definitions
        """
        sa = StepArgument.get(self._symbol)
        if sa:
            return sa.get(name)

    def _property_path_reader(self, src, encoding="utf-8"):
        """
        Returns an resource contents from the path if src starts with "path:", returns src if not
        """
        if src[:5].upper() == "PATH:":
            fpath = src[5:]
            if os.path.exists(fpath) is False:
                raise FileNotFound(src)
            with open(fpath, mode="r", encoding=encoding) as f:
                return f.read()
        return src

    def _exception_dispatcher(self, e):
        """
        Handle and dispath CliboaExceptions
        """
        # TODO Currently not doing anything
        raise e


class BaseSqlite(BaseStep):
    """
    Base class of all the sqlite classes
    """

    def __init__(self):
        super().__init__()
        self._sqlite_adptr = SqliteAdapter()
        self._dbname = None
        self._columns = []
        self._vacuum = False

    @property
    def dbname(self):
        return self._dbname

    @dbname.setter
    def dbname(self, dbname):
        self._dbname = dbname

    @property
    def columns(self):
        return self._columns

    @columns.setter
    def columns(self, columns):
        self._columns = columns

    @property
    def vacuum(self):
        return self._vacuum

    @vacuum.setter
    def vacuum(self, vacuum):
        self._vacuum = vacuum

    def execute(self, *args):
        # essential parameters check
        param_valid = EssentialParameters(self.__class__.__name__, [self._dbname])
        param_valid()

    def _dict_factory(self, cursor, row):
        d = {}
        for i, col in enumerate(cursor.description):
            d[col[0]] = row[i]
        return d

    def _close_database(self):
        """
        Disconnect sqlite database (execute vacuume if necessary)
        """
        self._sqlite_adptr.close()
        if self._vacuum is True:
            try:
                self._sqlite_adptr.connect(self._dbname)
                self._sqlite_adptr.execute("VACUUM")
            finally:
                self._sqlite_adptr.close()


class SqliteQueryExecute(BaseSqlite):
    """
    Execute only row query of insert, update or delete
    If would like to execute read query, use SqliteRead class
    """

    def __init__(self):
        super().__init__()
        self.__tblname = None
        self.__raw_query = None

    @property
    def tblname(self):
        return self.__tblname

    @tblname.setter
    def tblname(self, tblname):
        self.__tblname = tblname

    @property
    def raw_query(self):
        return self.__raw_query

    @raw_query.setter
    def raw_query(self, raw_query):
        self.__raw_query = raw_query

    def execute(self, *args):
        super().execute()
        self._sqlite_adptr.connect(self._dbname)
        self._sqlite_adptr.execute(self.__raw_query)
        self._sqlite_adptr.commit()


class Stdout(BaseStep):
    """
    Standard output for io: input
    """

    def __init__(self):
        super().__init__()

    def execute(self, *args):
        output_valid = IOOutput(self._io)
        output_valid()

        with open(self._s.cache_file, "r", encoding="utf-8") as f:
            for l in f:
                print(l)
