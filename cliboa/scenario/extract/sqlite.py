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
from cliboa.scenario.sqlite import BaseSqlite
from cliboa.scenario.validator import EssentialParameters


class SqliteExport(BaseSqlite):
    def __init__(self):
        super().__init__()
        self._tblname = None
        self._dest_path = None
        self._encoding = "utf-8"
        self._order = []
        self._no_duplicate = False

    def tblname(self, tblname):
        self._tblname = tblname

    def dest_path(self, dest_path):
        self._dest_path = dest_path

    def encoding(self, encoding):
        self._encoding = encoding

    def order(self, order):
        self._order = order

    def no_duplicate(self, no_duplicate):
        self._no_duplicate = no_duplicate

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(self.__class__.__name__, [self._dest_path])
        valid()

        self._sqlite_adptr.connect(self._dbname)
        try:
            self._sqlite_adptr.export_table(
                self._tblname,
                self._dest_path,
                encoding=self._encoding,
                order=self._order,
                no_duplicate=self._no_duplicate,
            )
        finally:
            self._close_database()
