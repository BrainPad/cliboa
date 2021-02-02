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
import os

from cliboa.scenario.load.sqlite import SqliteImport
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog
from cliboa.util.sqlite import SqliteAdapter


class TestSqliteImport(object):

    DB_NAME = "test.db"
    TBL_NAME = "foo"

    def test_ok_1(self):
        """
        Input csv file is only one
        """
        TEST_FILE = "sqlite_write_test.csv"
        try:
            with open(TEST_FILE, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["No", "TEXT"])
                writer.writerow(["1", "A"])

            instance = self._create_instance(TEST_FILE, True)
            instance.execute()

            adapter = SqliteAdapter()
            adapter.connect(self.DB_NAME)
            cur = adapter.fetch(
                "SELECT * FROM %s" % self.TBL_NAME, row_factory=self._dict_factory
            )

            count = 0
            for row in cur:
                if count == 0:
                    assert row == {"No": "1", "TEXT": "A"}
                count += 1
        finally:
            self._clean(self.DB_NAME)
            self._clean(TEST_FILE)

    def test_ok_2(self):
        """
        Input csv file is plural
        """
        TEST_FILE_1 = "sqlite_write_test_1.csv"
        TEST_FILE_2 = "sqlite_write_test_2.csv"
        try:
            with open(TEST_FILE_1, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["No", "TEXT"])
                writer.writerow(["1", "A"])

            with open(TEST_FILE_2, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["No", "TEXT"])
                writer.writerow(["2", "B"])

            instance = self._create_instance(r"sqlite_write_test_.*.csv", True)
            instance.execute()

            adapter = SqliteAdapter()
            adapter.connect(self.DB_NAME)
            cur = adapter.fetch(
                "SELECT * FROM %s" % self.TBL_NAME, row_factory=self._dict_factory
            )

            count = 0
            for row in cur:
                if count == 0:
                    assert row == {"No": "1", "TEXT": "A"}
                elif count == 1:
                    assert row == {"No": "2", "TEXT": "B"}
                count += 1

        finally:
            self._clean(self.DB_NAME)
            self._clean(TEST_FILE_1)
            self._clean(TEST_FILE_2)

    def test_ok_3(self):
        """
        Input csv file is plural.
        Csv files format are not the same.
        """
        TEST_FILE_1 = "sqlite_write_test_1.csv"
        TEST_FILE_2 = "sqlite_write_test_2.csv"
        try:
            with open(TEST_FILE_1, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["No", "TEXT"])
                writer.writerow(["1", "A"])

            with open(TEST_FILE_2, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["No", "NAME"])
                writer.writerow(["2", "John"])

            instance = self._create_instance(r"sqlite_write_test_.*.csv", True)
            instance.execute()

            adapter = SqliteAdapter()
            adapter.connect(self.DB_NAME)
            cur = adapter.fetch(
                "SELECT * FROM %s" % self.TBL_NAME, row_factory=self._dict_factory
            )

            count = 0
            for row in cur:
                if count == 0:
                    assert row == {"No": "1", "TEXT": "A", "NAME": None}
                elif count == 1:
                    assert row == {"No": "2", "TEXT": None, "NAME": "John"}
                count += 1

        finally:
            self._clean(self.DB_NAME)
            self._clean(TEST_FILE_1)
            self._clean(TEST_FILE_2)

    def test_ok_4(self):
        """
        Input csv file is only one.
        Sqlite database already exists.
        Csv columns and table columns are the same.
        """
        TEST_FILE_1 = "sqlite_write_test_1.csv"
        TEST_FILE_2 = "sqlite_write_test_2.csv"
        try:
            with open(TEST_FILE_1, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["No", "TEXT"])
                writer.writerow(["1", "A"])

            with open(TEST_FILE_2, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["No", "TEXT"])
                writer.writerow(["2", "B"])

            instance = self._create_instance(TEST_FILE_1, True)
            instance.execute()

            instance = self._create_instance(TEST_FILE_2, False)
            instance.execute()

            adapter = SqliteAdapter()
            adapter.connect(self.DB_NAME)
            cur = adapter.fetch(
                "SELECT * FROM %s" % self.TBL_NAME, row_factory=self._dict_factory
            )

            count = 0
            for row in cur:
                if count == 0:
                    assert row == {"No": "1", "TEXT": "A"}
                elif count == 1:
                    assert row == {"No": "2", "TEXT": "B"}
                count += 1
        finally:
            self._clean(self.DB_NAME)
            self._clean(TEST_FILE_1)
            self._clean(TEST_FILE_2)

    def test_ok_5(self):
        """
        Input csv file is only one.
        Sqlite database already exists.
        Csv columns and table columns are not the same.
        force_insert is True.
        """
        TEST_FILE_1 = "sqlite_write_test_1.csv"
        TEST_FILE_2 = "sqlite_write_test_2.csv"
        try:
            with open(TEST_FILE_1, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["No", "TEXT"])
                writer.writerow(["1", "A"])

            with open(TEST_FILE_2, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["No", "NAME"])
                writer.writerow(["2", "John"])

            instance = self._create_instance(TEST_FILE_1, True)
            instance.execute()

            instance = self._create_instance(TEST_FILE_2, False)
            Helper.set_property(instance, "force_insert", True)
            instance.execute()

            adapter = SqliteAdapter()
            adapter.connect(self.DB_NAME)
            cur = adapter.fetch(
                "SELECT * FROM %s" % self.TBL_NAME, row_factory=self._dict_factory
            )

            count = 0
            for row in cur:
                if count == 0:
                    assert row == {"No": "1", "TEXT": "A", "NAME": None}
                elif count == 1:
                    assert row == {"No": "2", "TEXT": None, "NAME": "John"}
                count += 1
        finally:
            self._clean(self.DB_NAME)
            self._clean(TEST_FILE_1)
            self._clean(TEST_FILE_2)

    def test_ok_6(self):
        """
        Input csv file is only one.
        Sqlite database already exists.
        refresh is False.
        """
        TEST_FILE_1 = "sqlite_write_test_1.csv"
        TEST_FILE_2 = "sqlite_write_test_2.csv"
        try:
            with open(TEST_FILE_1, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["No", "TEXT"])
                writer.writerow(["1", "A"])

            with open(TEST_FILE_2, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["No", "TEXT"])
                writer.writerow(["2", "B"])

            instance = self._create_instance(TEST_FILE_1, False)
            instance.execute()

            instance = self._create_instance(TEST_FILE_2, False)
            instance.execute()

            adapter = SqliteAdapter()
            adapter.connect(self.DB_NAME)
            cur = adapter.fetch(
                "SELECT * FROM %s" % self.TBL_NAME, row_factory=self._dict_factory
            )

            count = 0
            for row in cur:
                if count == 0:
                    assert row == {"No": "1", "TEXT": "A"}
                elif count == 1:
                    assert row == {"No": "2", "TEXT": "B"}
                count += 1
        finally:
            self._clean(self.DB_NAME)
            self._clean(TEST_FILE_1)
            self._clean(TEST_FILE_2)

    def test_ng_1(self):
        """
        Input csv file is only one.
        Sqlite database already exists.
        Csv columns and table columns are not the same.
        force_insert is False.
        """
        TEST_FILE_1 = "sqlite_write_test_1.csv"
        TEST_FILE_2 = "sqlite_write_test_2.csv"
        try:
            with open(TEST_FILE_1, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["No", "TEXT"])
                writer.writerow(["1", "A"])

            with open(TEST_FILE_2, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["No", "NAME"])
                writer.writerow(["2", "John"])

            instance = self._create_instance(TEST_FILE_1, True)
            instance.execute()

            instance = self._create_instance(TEST_FILE_2, False)

            try:
                instance.execute()
            except Exception as e:
                assert "were not matched to table column " in str(e)

        finally:
            self._clean(self.DB_NAME)
            self._clean(TEST_FILE_1)
            self._clean(TEST_FILE_2)

    def _dict_factory(self, cursor, row):
        d = {}
        for i, col in enumerate(cursor.description):
            d[col[0]] = row[i]
        return d

    def _create_instance(self, pattern, refresh):
        instance = SqliteImport()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "dbname", self.DB_NAME)
        Helper.set_property(instance, "src_dir", ".")
        Helper.set_property(instance, "src_pattern", pattern)
        Helper.set_property(instance, "tblname", self.TBL_NAME)
        Helper.set_property(instance, "refresh", refresh)
        return instance

    def _clean(self, path):
        if os.path.exists(path):
            os.remove(path)
