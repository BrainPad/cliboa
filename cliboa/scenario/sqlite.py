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
from cliboa.scenario.base import BaseStep
from cliboa.scenario.validator import EssentialParameters
from cliboa.util.constant import StepStatus


class BaseSqlite(BaseStep):
    """
    Base class of all the sqlite classes
    """

    def __init__(self):
        super().__init__()
        self._sqlite_adptr = SqliteAdapter()
        self._dbname = None
        self._vacuum = False

    def dbname(self, dbname):
        self._dbname = dbname

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
        Disconnect sqlite database (execute vacuum if necessary)
        """
        self._sqlite_adptr.close()
        if self._vacuum is True:
            try:
                self._sqlite_adptr.connect(self._dbname)
                self._sqlite_adptr.execute("VACUUM")
            finally:
                self._sqlite_adptr.close()


class SqliteTransaction(BaseSqlite):
    """
    Base class of sqlite transaction
    """

    def __init__(self):
        super().__init__()

    def execute(self, *args):
        """
        Execute db connection, commit, close, vacuum

        Args:
            args[0]: db operation function to execute as transaction
        """

        # When args is not specified, just call validation of base class
        if not args:
            super().execute()
            return StepStatus.ABNORMAL_TERMINATION
        func = args[0]

        # TODO
        # Add to handle to select DB transaction seperation level
        self._logger.info("Start DB Transaction")
        self._sqlite_adptr.connect(self._dbname)
        try:
            func()
            self._sqlite_adptr.commit()
        finally:
            super()._close_database()
        self._logger.info("Finish DB Transaction")


class SqliteQueryExecute(SqliteTransaction):
    """
    Execute only row query of insert, update or delete
    """

    def __init__(self):
        super().__init__()
        self._raw_query = None

    def raw_query(self, raw_query):
        self._raw_query = raw_query

    def execute(self, *args):
        param_valid = EssentialParameters(self.__class__.__name__, [self._raw_query])
        param_valid()

        def func():
            self._sqlite_adptr.execute(self._raw_query)

        super().execute(func)
