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
from pydantic import Field

from cliboa.adapter.csv import Csv
from cliboa.scenario.file import FileRead
from cliboa.scenario.sqlite import SqliteTransaction
from cliboa.util.base import _warn_deprecated_args
from cliboa.util.exception import CliboaException, FileNotFound


class SqliteImport(SqliteTransaction, FileRead):
    class Arguments(SqliteTransaction.Arguments, FileRead.Arguments):
        tblname: str
        primary_key: str | None = None
        index: list[str] = Field(default_factory=list)
        refresh: bool = True
        force_insert: bool = False

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _tblname(self):
        return self.args.tblname

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _primary_key(self):
        return self.args.primary_key

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _index(self):
        return self.args.index

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _refresh(self):
        return self.args.refresh

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _force_insert(self):
        return self.args.force_insert

    def process(self):
        files = self.get_src_files()
        if not self.check_file_existence(files):
            raise FileNotFound("No csv file was found.")

        files.sort()
        # Find csv columns from all csv files
        csv_columns = []
        for file in files:
            csv_columns.extend(Csv.get_column_names(file))
        csv_columns = sorted(set(csv_columns), key=csv_columns.index)

        if self.args.refresh is True:
            # Drop table in advance, If refresh is True
            self._sqlite_adptr.drop_table(self.args.tblname)
            self._sqlite_adptr.create_table(self.args.tblname, csv_columns, self.args.primary_key)
        else:
            self._sqlite_adptr.create_table(self.args.tblname, csv_columns, self.args.primary_key)

            if self.args.force_insert is True:
                db_columns = self._sqlite_adptr.get_column_names(self.args.tblname)
                result = list(set(csv_columns) - set(db_columns))
                self._sqlite_adptr.add_columns(self.args.tblname, result)
            else:
                # Make sure if csv columns and db table names are exactly the same
                db_columns = self._sqlite_adptr.get_column_names(self.args.tblname)
                if self._sqlite_adptr.escape_columns(
                    csv_columns
                ) != self._sqlite_adptr.escape_columns(db_columns):
                    raise CliboaException(
                        "Csv columns %s were not matched to table column %s."
                        % (csv_columns, db_columns)
                    )

        for file in files:
            self._sqlite_adptr.import_table(
                file, self.args.tblname, refresh=False, encoding=self.args.encoding
            )

        if self.args.index and len(self.args.index) > 0:
            """
            Create index (Add the index at the end for
            better performance when insert data is large)
            """
            self._sqlite_adptr.add_index(self.args.tblname, self.args.index)
