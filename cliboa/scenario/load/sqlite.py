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
from cliboa.scenario.sqlite import SqliteTransaction
from cliboa.scenario.validator import EssentialParameters
from cliboa.util.csv import Csv
from cliboa.util.exception import CliboaException, FileNotFound


class SqliteImport(SqliteTransaction):
    def __init__(self):
        super().__init__()
        self._src_dir = None
        self._src_pattern = None
        self._tblname = None
        self._primary_key = None
        self._index = []
        self._refresh = True
        self._force_insert = False
        self._encoding = "utf-8"

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def tblname(self, tblname):
        self._tblname = tblname

    def primary_key(self, primary_key):
        self._primary_key = primary_key

    def index(self, index):
        self._index = index

    def refresh(self, refresh):
        self._refresh = refresh

    def force_insert(self, force_insert):
        self._force_insert = force_insert

    def encoding(self, encoding):
        self._encoding = encoding

    def execute(self, *args):
        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern, self._tblname]
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self._logger.info("Files found %s" % files)

        if len(files) == 0:
            raise FileNotFound("No csv file was found.")

        files.sort()

        def func():
            # Find csv columns from all csv files
            csv_columns = []
            for file in files:
                csv_columns.extend(Csv.get_column_names(file))
            csv_columns = sorted(set(csv_columns), key=csv_columns.index)

            if self._refresh is True:
                # Drop table in advance, If refresh is True
                self._sqlite_adptr.drop_table(self._tblname)
                self._sqlite_adptr.create_table(self._tblname, csv_columns, self._primary_key)
            else:
                self._sqlite_adptr.create_table(self._tblname, csv_columns, self._primary_key)

                if self._force_insert is True:
                    db_columns = self._sqlite_adptr.get_column_names(self._tblname)
                    result = list(set(csv_columns) - set(db_columns))
                    self._sqlite_adptr.add_columns(self._tblname, result)
                else:
                    # Make sure if csv columns and db table names are exactly the same
                    db_columns = self._sqlite_adptr.get_column_names(self._tblname)
                    if self._sqlite_adptr.escape_columns(
                        csv_columns
                    ) != self._sqlite_adptr.escape_columns(db_columns):
                        raise CliboaException(
                            "Csv columns %s were not matched to table column %s."
                            % (csv_columns, db_columns)
                        )

            for file in files:
                self._sqlite_adptr.import_table(
                    file, self._tblname, refresh=False, encoding=self._encoding
                )

            if self._index and len(self._index) > 0:
                """
                Create index (Add the index at the end for
                better performance when insert data is large)
                """
                self._sqlite_adptr.add_index(self._tblname, self._index)

        super().execute(func)
