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
import os

from pydantic import Field

from cliboa.scenario.sqlite import BaseSqlite
from cliboa.util.base import _warn_deprecated_args


class SqliteExport(BaseSqlite):
    class Arguments(BaseSqlite.Arguments):
        tblname: str
        dest_path: str
        encoding: str = "utf-8"
        order: list[str] = Field(default_factory=list)
        no_duplicate: bool = False

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _tblname(self):
        return self.args.tblname

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _dest_path(self):
        return self.args.dest_path

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _encoding(self):
        return self.args.encoding

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _order(self):
        return self.args.order

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _no_duplicate(self):
        return self.args.no_duplicate

    def execute(self, *args):
        dest_dir = os.path.dirname(self.args.dest_path)
        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)

        self._sqlite_adptr.connect(self.args.dbname)
        try:
            self._sqlite_adptr.export_table(
                self.args.tblname,
                self.args.dest_path,
                encoding=self.args.encoding,
                order=self.args.order,
                no_duplicate=self.args.no_duplicate,
            )
        finally:
            self._close_database()
