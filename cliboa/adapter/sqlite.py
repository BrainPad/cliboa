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
import csv
import sqlite3

from cliboa.util.lisboa_log import LisboaLog


class SqliteAdapter(object):

    _COMMIT_COUNT = 500

    """
    Adapter class of sqlite3
    """

    def __init__(self):
        self._logger = LisboaLog.get_logger(__name__)
        self._cur = None
        self._con = None

    def connect(self, dbname):
        """
        Get sqlite connection
        """
        self._con = sqlite3.connect(dbname)
        self._con.execute("PRAGMA synchronous = OFF")
        self._con.execute("PRAGMA journal_mode = OFF")

    def close(self):
        """
        Release sqlite connection
        """
        if self._cur:
            self._cur.close()
            self._cur = None

        if self._con:
            self._con.close()

    def fetch(self, sql, row_factory=None):
        """
        Get cursor after executed query of select-related

        Args:
            sql (str): SQL

        Returns:
            cursor: cursor
        """
        if self._cur:
            self._cur.close()
            self._cur = None
        self._cur = self._con.cursor()
        if row_factory:
            self._cur.row_factory = row_factory
        self._cur.execute(sql)
        return self._cur

    def create_user_func(self, dict):
        """
        Create function

        Args:
            dict: (dict)
                name: Name of user defined function.
                args: Number of arguments.
                func: User defined function.
        """
        self._con.create_function(dict["name"], dict["args"], dict["func"])

    def execute(self, sql):
        """
        Execute SQL

        Args:
            sql (str): SQL to execute
        """
        self._con.execute(sql)

    def commit(self):
        """
        Commit
        """
        self._con.commit()

    def execute_many_insert(self, tblname, column_def, insert_rows, is_replace_into=True):
        """
        Execute many INSERT INTO SQL

        Args:
            tblname: target table
            column_def: table column definition
            insert_rows(dict[])  rows to be inserted
            is_replace_into: when using replace into: True,
                             when using insert into: False
        """
        if not tblname or not insert_rows:
            raise ValueError("Parameters are missing")

        columns = ",".join(self.escape_columns(column_def))
        holders = "?" * len(column_def)
        insert_sql = "REPLACE INTO" if is_replace_into else "INSERT INTO"
        sql = insert_sql + " %s (%s) VALUES (%s)" % (
            tblname,
            columns,
            ",".join(list(holders)),
        )
        self._logger.debug("sql: %s" % sql)
        values = []
        for row in insert_rows:
            vs = []
            for c in column_def:
                vs.append(row.get(c))
            values.append(vs)

            if len(vs) != len(column_def):
                raise ValueError(
                    "The length of insert rows must be equal to the column definition. Column definition: %s, Insert rows: %s"  # noqa
                    % (column_def, vs)
                )
        self._con.executemany(sql, values)

    def create_table(self, tblname, columns, primary_key=None):
        """
        Create table, if it is not exist.

        Args:
            tblname (str): table name
            columns (str[]): column names
            primary_key=None (str) primary key
        """
        self._logger.info("Create table [%s]" % tblname)

        columns = self.escape_columns(columns)
        if primary_key is None:
            sql = "CREATE TABLE IF NOT EXISTS %s (%s)"
            self.execute(sql % (tblname, " TEXT, ".join(columns) + " TEXT"))
        else:
            sql = "CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY(%s))"
            self.execute(sql % (tblname, " TEXT, ".join(columns) + " TEXT", primary_key))
        self.commit()

    def drop_table(self, tblname):
        """
        Drop table, if exist.

        Args:
            tblname (str): table name
        """
        self.execute("DROP TABLE IF EXISTS %s" % tblname)
        self.commit()

    def add_columns(self, tblname, columns):
        """
        Add columns by alter table query.

        tblname (str): table name.
        columns (str[]): column names that is to be added.
        """
        for column in columns:
            self.execute("ALTER TABLE %s ADD COLUMN '%s' text" % (tblname, column))

        self.commit()

    def add_index(self, tblname, columns):
        """
        Create index

        Args:
            tblname (str): target table
            columns (str[]): index list
        """
        idx_name = tblname + "_" + "".join(columns)
        self._logger.info("Add index: %s" % idx_name)
        sql = "CREATE INDEX %s ON %s(%s)" % (idx_name, tblname, ",".join(columns))
        self.execute(sql)
        self.commit()

    def get_column_names(self, tblname):
        """
        Returns column names.

        Args:
            tblname (str): target table
        """
        cur = self.fetch("PRAGMA TABLE_INFO(%s)" % tblname)
        return [x[1] for x in cur]

    def import_table(self, src, tblname, refresh=True, encoding="utf-8", delimiter=","):
        """
        Create new table and import all data from csv(tsv).

        Args:
            src (str): csv file path
            tblname (str): table name
            refresh=True (bool): Drop table in advance, if table already exists
            encoding="utf-8" (str) Encoding
            delimiter="," (str) Set \t if src file is tsv
        """

        if refresh is True:
            self.drop_table(tblname)

        with open(src, mode="r", encoding=encoding) as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            # Table columns will be the same with csv column names.

            columns = reader.fieldnames
            self.create_table(tblname, columns)

            # Put all csv records into the table.
            self._logger.info("Insert all csv records into table[%s]" % tblname)
            params = []
            count = 0
            for row in reader:
                params.append(row)
                count = count + 1
                if len(params) == self._COMMIT_COUNT:
                    self.execute_many_insert(tblname, columns, params)
                    self.commit()
                    params.clear()
            if len(params) > 0:
                self.execute_many_insert(tblname, columns, params)
                self.commit()

            self._logger.info("Insert finished. total record: %s" % count)

    def export_table(
        self,
        tblname,
        dest,
        quoting=csv.QUOTE_ALL,
        encoding="utf-8",
        order=[],
        delimiter=",",
        no_duplicate=False,
    ):
        """
        Export all data from specific table and output as csv.

        Args:
            tblname (str): table name
            dest (str): output csv file path
            encoding="utf-8" (str) Encoding
            order=[] (array) orders of outputting data
            delimiter="," (str) Set \t if src file is tsv
            no_duplicate=False (bool) If true is set, get data with "select distinct"
        """
        if no_duplicate:
            sql = "SELECT DISTINCT * FROM '%s'" % tblname
        else:
            sql = "SELECT * FROM '%s'" % tblname

        if order and len(order) > 0:
            sql += " ORDER BY " + ",".join(order)

        with open(dest, "wt", encoding=encoding, newline="") as f:
            cur = self.fetch(sql)
            fieldnames = [d[0] for d in cur.description]
            writer = csv.writer(f, quoting=quoting, delimiter=delimiter)
            writer.writerow(fieldnames)

            for row in cur:
                writer.writerow(row)
            f.flush()

    def escape_columns(self, columns):
        return ["`%s`" % column for column in columns]
