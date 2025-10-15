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

import pytest

from cliboa.adapter.sqlite import SqliteAdapter
from cliboa.scenario.extract.sqlite import SqliteExport
from cliboa.util.helper import Helper
from cliboa.util.log import _get_logger


class TestSqliteExport(object):

    _DB_NAME = "test.db"
    _TBL_NAME = "foo"
    _RESULT_FILE = "result.csv"

    def test_ok_1(self):
        """
        Insert one record to a table and export the table.
        """
        test_data = [{"No": 1, "TEXT": "AAA"}]

        try:
            self._insert_test_data(test_data)

            instance = self._create_instance()
            instance.execute()

            with open(self._RESULT_FILE, "r") as o:
                reader = csv.DictReader(o)
                for row in reader:
                    assert "1" == row.get("No")
                    assert "AAA" == row.get("TEXT")
        finally:
            self._clean(self._DB_NAME)
            self._clean(self._RESULT_FILE)

    def test_ok_2(self):
        """
        Insert plural records to a table and export the table with order by option.
        """
        test_data = [
            {"No": 1, "TEXT": "AAA"},
            {"No": 3, "TEXT": "CCC"},
            {"No": 2, "TEXT": "BBB"},
        ]

        try:
            self._insert_test_data(test_data)

            instance = self._create_instance()
            Helper.set_property(instance, "order", ["No"])
            instance.execute()

            with open(self._RESULT_FILE, "r") as o:
                reader = csv.DictReader(o)
                for i, row in enumerate(reader):
                    if i == 0:
                        assert "1" == row.get("No")
                        assert "AAA" == row.get("TEXT")
                    elif i == 1:
                        assert "2" == row.get("No")
                        assert "BBB" == row.get("TEXT")
                    elif i == 2:
                        assert "3" == row.get("No")
                        assert "CCC" == row.get("TEXT")
                    else:
                        pytest.fail("Must not be reached")
        finally:
            self._clean(self._DB_NAME)
            self._clean(self._RESULT_FILE)

    def test_ok_3(self):
        """
        Insert plural records(some records are the same)
         to a table and export the table with no_duplicate option.
        """
        test_data = [
            {"No": 1, "TEXT": "AAA"},
            {"No": 2, "TEXT": "BBB"},
            {"No": 2, "TEXT": "BBB"},
        ]

        try:
            self._insert_test_data(test_data)

            instance = self._create_instance()
            Helper.set_property(instance, "order", ["No"])
            Helper.set_property(instance, "no_duplicate", True)
            instance.execute()

            with open(self._RESULT_FILE, "r") as o:
                reader = csv.DictReader(o)
                for i, row in enumerate(reader):
                    if i == 0:
                        assert "1" == row.get("No")
                        assert "AAA" == row.get("TEXT")
                    elif i == 1:
                        assert "2" == row.get("No")
                        assert "BBB" == row.get("TEXT")
                    else:
                        pytest.fail("Must not be reached")

        finally:
            self._clean(self._DB_NAME)
            self._clean(self._RESULT_FILE)

    def _create_instance(self):
        instance = SqliteExport()
        Helper.set_property(instance, "logger", _get_logger(__name__))
        Helper.set_property(instance, "dbname", self._DB_NAME)
        Helper.set_property(instance, "dest_path", self._RESULT_FILE)
        Helper.set_property(instance, "tblname", self._TBL_NAME)
        return instance

    def _clean(self, path):
        if os.path.exists(path):
            os.remove(path)

    def _insert_test_data(self, obj):
        adapter = SqliteAdapter()
        adapter.connect(self._DB_NAME)
        try:
            adapter.drop_table(self._TBL_NAME)
            adapter.create_table(self._TBL_NAME, ["No", "TEXT"])
            adapter.execute_many_insert(self._TBL_NAME, ["No", "TEXT"], obj)
            adapter.commit()
        finally:
            adapter.close()
