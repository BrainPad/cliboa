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
from pydantic import BaseModel

from cliboa.adapter.sqlite import SqliteAdapter
from cliboa.scenario.base import BaseStep
from cliboa.util.base import _warn_deprecated_args
from cliboa.util.constant import StepStatus


class BaseSqlite(BaseStep):
    """
    Base class of all the sqlite classes
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._sqlite_adptr = self._resolve("adapter_sqlite", SqliteAdapter)

    class Arguments(BaseModel):
        dbname: str
        vacuum: bool = False

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _dbname(self):
        return self.args.dbname

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _vacuum(self):
        return self.args.vacuum

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
        if self.args.vacuum is True:
            try:
                self._sqlite_adptr.connect(self.args.dbname)
                self._sqlite_adptr.execute("VACUUM")
            finally:
                self._sqlite_adptr.close()


class SqliteTransaction(BaseSqlite):
    """
    Base class of sqlite transaction
    """

    def execute(self, *args):
        """
        Execute db connection, commit, close, vacuum

        Args:
            args[0]: db operation function to execute as transaction
        """

        # TODO
        # Add to handle to select DB transaction seperation level
        self.logger.info("Start DB Transaction")
        self._sqlite_adptr.connect(self.args.dbname)
        try:
            self.process()
            self._sqlite_adptr.commit()
        except NotImplementedError:
            return StepStatus.ABNORMAL_TERMINATION
        finally:
            super()._close_database()
        self.logger.info("Finish DB Transaction")

    def process(self):
        raise NotImplementedError()


class SqliteQueryExecute(SqliteTransaction):
    """
    Execute only row query of insert, update or delete
    """

    class Arguments(SqliteTransaction.Arguments):
        raw_query: str

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def raw_query(self, raw_query):
        self.args.raw_query = raw_query

    def process(self):
        self._sqlite_adptr.execute(self.args.raw_query)
