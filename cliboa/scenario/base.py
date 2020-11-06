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
import configparser
import os
import re
from abc import abstractmethod

from cliboa.conf import env
from cliboa.scenario.validator import EssentialParameters, IOOutput, SqliteTableExistence  # noqa
from cliboa.util.cache import StepArgument, StorageIO
from cliboa.util.exception import FileNotFound
from cliboa.util.file import File
from cliboa.util.sqlite import SqliteAdapter


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
        self._listeners = []

    def step(self, step):
        self._step = step

    def symbol(self, symbol):
        self._symbol = symbol

    def parallel(self, parallel):
        self._parallel = parallel

    def io(self, io):
        self._io = io

    def logger(self, logger):
        self._logger = logger

    def listeners(self, listeners):
        self._listeners = listeners

    def trigger(self, *args):
        mask = None
        path = os.path.join(env.BASE_DIR, "conf", "cliboa.ini")
        if os.path.exists(path):
            try:
                conf = configparser.ConfigParser()
                conf.read(path, encoding="utf-8")
                mask = conf.get("logging", "mask")
                pattern = re.compile(mask)
            except Exception as e:
                self._logger.warning(e)

        for k, v in self.__dict__.items():
            if mask is not None and pattern.search(k):
                self._logger.info("%s : ****" % k)
            else:
                self._logger.info("%s : %s" % (k, v))
        try:
            for listener in self._listeners:
                listener.before_step(self)

            ret = self.execute(args)

            for listener in self._listeners:
                listener.after_step(self)

            return ret

        except Exception as e:
            for listener in self._listeners:
                listener.error_step(self, e)

            return self._exception_dispatcher(e)
        finally:
            for listener in self._listeners:
                listener.after_completion(self)

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
        Returns an resource contents from the path if src starts with "path:",
        returns src if not
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

    def dbname(self, dbname):
        self._dbname = dbname

    def columns(self, columns):
        self._columns = columns

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
        self._tblname = None
        self._raw_query = None

    def tblname(self, tblname):
        self._tblname = tblname

    def raw_query(self, raw_query):
        self._raw_query = raw_query

    def execute(self, *args):
        super().execute()
        self._sqlite_adptr.connect(self._dbname)
        self._sqlite_adptr.execute(self._raw_query)
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
