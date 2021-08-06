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
import ast
import codecs
import csv

from cliboa.scenario.sqlite import SqliteTransaction
from cliboa.scenario.validator import (
    EssentialParameters,
    IOOutput,
    SqliteTableExistence,
)
from cliboa.util.csv import Csv
from cliboa.util.exception import CliboaException, FileNotFound, SqliteInvalid


class SqliteCreation(SqliteTransaction):
    """
    @deprecated
    Please do not use this class.

    Insert all the input data specified as 'io: input' in yaml to the specified table.
    """

    def __init__(self):
        super().__init__()
        self._tblname = None
        self._columns = []
        self._replace_into = True
        self._insert_cnt = 10
        self._primary_key = None
        self._refresh = False

    def tblname(self, tblname):
        self._tblname = tblname

    def columns(self, columns):
        self._columns = columns

    def replace_into(self, replace_into):
        self._replace_into = replace_into

    def insert_cnt(self, insert_cnt):
        self._insert_cnt = insert_cnt

    def primary_key(self, primary_key):
        self._primary_key = primary_key

    def refresh(self, refresh):
        self._refresh = refresh

    def execute(self, *args):
        self._logger.warning("Deprecated. Please do not use this class.")

        super().execute()

        param_valid = EssentialParameters(self.__class__.__name__, [self._tblname])
        param_valid()

        tbl_valid = SqliteTableExistence(self._dbname, self._tblname)
        tbl_valid()

        output_valid = IOOutput(self._io)
        output_valid()

        # get table column definition
        self._sqlite_adptr.connect(self._dbname)
        column_def = self.__get_column_def()

        if self._refresh is True:
            self.__refresh_table(column_def)

        # database transaction
        def insert():
            self._logger.info("Start to insert")
            insert_rows = []
            with open(self._s.cache_file, "r", encoding="utf-8") as f:
                for i, l_str in enumerate(f, 1):
                    l_dict = ast.literal_eval(l_str)
                    insert_rows.append(l_dict)

                    # Check only once
                    if i == 1:
                        self.__valid_column_def(column_def, l_dict)

                    # execute bulk insert
                    if i % self._insert_cnt == 0:
                        self._sqlite_adptr.execute_many_insert(
                            self._tblname, column_def, insert_rows, self._replace_into
                        )
                        insert_rows.clear()

                if len(insert_rows) > 0:
                    self._sqlite_adptr.execute_many_insert(
                        self._tblname, column_def, insert_rows, self._replace_into
                    )
                    insert_rows.clear()

            self._logger.info("Finish to insert")

        super().execute(insert)
        self._s.remove()

    def __get_column_def(self):
        """
        Get table column definition
        """
        if self._columns:
            return self._columns

        result = self._sqlite_adptr.fetch("PRAGMA TABLE_INFO(%s)" % self._tblname)
        column_def = []
        for c in result:
            column_def.append(c[1])
        return column_def

    def __valid_column_def(self, column_def, l_dict):
        """
        Check column definitions in scenario.yml and cache file columns
        """
        l_keys_list = list(l_dict.keys())
        if column_def != l_keys_list:
            inconsistent_columns = set(column_def) ^ set(l_keys_list)
            raise SqliteInvalid(
                "The columns %s is not consistent between scenario.yml and input cache."
                % ", ".join(inconsistent_columns)
            )

    def __refresh_table(self, column_def):
        """
        Drop and recreate the table by using the existing table column definition
        """

        # def drop_and_create_tbl():
        self._logger.info("Drop table %s." % self._tblname)
        self._sqlite_adptr.execute("DROP TABLE IF EXISTS %s" % self._tblname)
        self._logger.info("Create new table %s." % self._tblname)

        if self._primary_key is None:
            sql = "CREATE TABLE IF NOT EXISTS %s (%s)"
            self._sqlite_adptr.execute(
                sql % (self._tblname, " TEXT, ".join(column_def) + " TEXT")
            )
        else:
            sql = "CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY(%s))"
            self._sqlite_adptr.execute(
                sql
                % (
                    self._tblname,
                    " TEXT, ".join(column_def) + " TEXT",
                    self._primary_key,
                )
            )


class CsvReadSqliteCreate(SqliteTransaction):
    """
    @deprecated
    """

    COMMIT_COUNT = 100

    def __init__(self):
        super().__init__()
        self._src_dir = None
        self._src_pattern = None
        self._tblname = None
        self._primary_key = None
        self._index = []
        self._refresh = True
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

    def encoding(self, encoding):
        self._encoding = encoding

    def execute(self, *args):
        self._logger.warning("Deprecated. Please do not use this class.")

        # essential parameters check
        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern, self._tblname]
        )
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        self._logger.info("Files found %s" % files)

        if len(files) > 1:
            raise Exception("Input file must be only one.")

        if len(files) == 0:
            raise FileNotFound("No csv file was found.")

        def func():
            if self._refresh is True:
                # Drop table in advance, If refresh is True
                self._sqlite_adptr.execute("DROP TABLE IF EXISTS %s" % self._tblname)
                self._sqlite_adptr.commit()

            with codecs.open(files[0], mode="r", encoding=self._encoding) as f:
                reader = csv.DictReader(f)
                # Table columns will be the same with csv column names.
                escaped_columns = ['"%s"' % fn for fn in reader.fieldnames]

                self._logger.info("Create table [%s]" % self._tblname)
                if self._primary_key is None:
                    sql = "CREATE TABLE IF NOT EXISTS %s (%s)"
                    self._sqlite_adptr.execute(
                        sql % (self._tblname, " TEXT, ".join(escaped_columns) + " TEXT")
                    )
                else:
                    sql = "CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY(%s))"
                    self._sqlite_adptr.execute(
                        sql
                        % (
                            self._tblname,
                            " TEXT, ".join(escaped_columns) + " TEXT",
                            self._primary_key,
                        )
                    )
                self._sqlite_adptr.commit()

                # Put all csv records into the table.
                self._logger.info(
                    "Insert all csv records into table[%s]" % self._tblname
                )
                params = []
                for row in reader:
                    params.append(row)
                    if len(params) == self.COMMIT_COUNT:
                        self._sqlite_adptr.execute_many_insert(
                            self._tblname, reader.fieldnames, params, False
                        )
                        self._sqlite_adptr.commit()
                        params.clear()
                if len(params) > 0:
                    self._sqlite_adptr.execute_many_insert(
                        self._tblname, reader.fieldnames, params, False
                    )
                    self._sqlite_adptr.commit()

            if self._index and len(self._index) > 0:
                """
                Create index (Add the index at the end for
                better performance when insert data is large)
                """
                self._logger.info("Add index")
                self._sqlite_adptr.add_index(self._tblname, self._index)
                self._sqlite_adptr.commit()

        super().execute(func)


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
                self._sqlite_adptr.create_table(
                    self._tblname, csv_columns, self._primary_key
                )
            else:
                self._sqlite_adptr.create_table(
                    self._tblname, csv_columns, self._primary_key
                )

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
