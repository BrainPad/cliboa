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
import json
import os
import pytest
import shutil

from glob import glob
from cliboa.conf import env
from cliboa.scenario.transform.json import (
    JsonlToCsvBase,
    JsonlToCsv
)
from cliboa.test import BaseCliboaTest
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog


class TestJsonTransform(BaseCliboaTest):
    def setUp(self):
        self._data_dir = os.path.join(env.BASE_DIR, "data")
        self._result_dir = os.path.join(env.BASE_DIR, "data", "result")
        os.makedirs(self._data_dir, exist_ok=True)
        os.makedirs(self._result_dir, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self._data_dir, ignore_errors=True)

    def _create_jsonl(self, data, fname="test.json"):
        path = os.path.join(self._data_dir, fname)
        mode = "a" if os.path.exists(path) else "w"
        with open(path, mode=mode, encoding="utf-8") as file:
            for d in data:
                json.dump(d, file, separators=(',', ':'))
                file.write('\n')


class TestJsonlToCsvBase(TestJsonTransform):
    class DummyJsonlToCsvBase(JsonlToCsvBase):
        def __init__(self):
            super().__init__()

        def execute(self, *args):
            super().execute()

        def convert_row(self, row):
            return [row]

    def test_execute_ok(self):
        # create test jsonl
        data = [
            {"id": "123456789", "name": "A", "age": "25", "value": []},
        ]
        self._create_jsonl(data)

        # set the essential attributes
        instance = JsonlToCsv()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.json")
        Helper.set_property(instance, "dest_dir", self._result_dir)
        instance.execute()
        dest = os.path.join(self._result_dir, "test.csv")
        with open(dest, mode="r") as f:
            reader = csv.DictReader(f, quoting=csv.QUOTE_ALL)
            self.assertEqual(1, len(list(reader)))
            for i, row in enumerate(reader):
                if i == 0:
                    self.assertEqual("123456789", row.get("id"))
                    self.assertEqual("A", row.get("name"))
                    self.assertEqual("25", row.get("age"))
                    self.assertEqual("[]", row.get("value"))


class TestJsonlToCsv(TestJsonTransform):
    def test_execute_ok(self):
        # create test jsonl
        data = [
            {"id": "123456789", "name": "A", "age": "25", "value": []},
            {"id": "1234567890",
             "name": "B",
             "age": "30",
             "value": [{"key": "test_key", "value": 999},
                       {"key": 'test"01"', "value": "true"}]},
        ]
        self._create_jsonl(data, "test_1.json")
        self._create_jsonl(data, "test_2.json")

        # set the essential attributes
        instance = JsonlToCsv()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.*.json")
        Helper.set_property(instance, "dest_dir", self._result_dir)
        instance.execute()
        files = glob(os.path.join(self._result_dir, "*.csv"))
        assert 2 == len(files)
        for file in files:
            with open(file, mode="r") as f:
                reader = csv.DictReader(f, quoting=csv.QUOTE_ALL)
                self.assertEqual(2, len(list(reader)))
                for i, row in enumerate(reader):
                    if i == 0:
                        self.assertEqual("123456789", row.get("id"))
                        self.assertEqual("A", row.get("name"))
                        self.assertEqual("25", row.get("age"))
                        self.assertEqual("[]", row.get("value"))
                    elif i == 1:
                        self.assertEqual("1234567890", row.get("id"))
                        self.assertEqual("B", row.get("name"))
                        self.assertEqual("30", row.get("age"))
                        self.assertEqual(
                            "[{'key': 'test_key', 'value': 999}, \
                             {'key': 'test\"01\"', 'value': 'true'}]",
                            row.get("value"))
                        assert row.endswith("\r\n")

    def test_execute_ok_2(self):
        # create test jsonl
        data = [
            {"id": "123456789", "name": "A", "age": "25", "value": []},
            {"id": "1234567890",
             "name": "B",
             "age": "30",
             "value": [{"key": "test,_key", "value": 999},
                       {"key": 'test"01"', "value": "true"}]},
        ]
        self._create_jsonl(data, "test_1.json")
        self._create_jsonl(data, "test_2.json")

        # set the essential attributes
        instance = JsonlToCsv()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.*.json")
        Helper.set_property(instance, "dest_dir", self._result_dir)
        Helper.set_property(instance, "quote", "QUOTE_NONE")
        Helper.set_property(instance, "escape_char", ",")
        instance.execute()
        files = glob(os.path.join(self._result_dir, "*.csv"))
        assert 2 == len(files)
        for file in files:
            with open(file, mode="r") as f:
                reader = csv.DictReader(f, quoting=csv.QUOTE_NONE)
                self.assertEqual(2, len(list(reader)))
                for i, row in enumerate(reader):
                    if i == 0:
                        self.assertEqual("123456789", row.get("id"))
                        self.assertEqual("A", row.get("name"))
                        self.assertEqual("25", row.get("age"))
                        self.assertEqual("[]", row.get("value"))
                    elif i == 1:
                        self.assertEqual("1234567890", row.get("id"))
                        self.assertEqual("B", row.get("name"))
                        self.assertEqual("30", row.get("age"))
                        self.assertEqual(
                            "[{'key': 'test,_key', 'value': 999}, \
                             {'key': 'test\"01\"', 'value': 'true'}]",
                            row.get("value"))

    def test_execute_ok_3(self):
        # create test jsonl
        data = [
            {"id": "123456789", "name": "A", "age": "25", "value": []},
        ]
        self._create_jsonl(data, "test_1.json")
        self._create_jsonl(data, "test_2.json")

        # set the essential attributes
        instance = JsonlToCsv()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.*.json")
        Helper.set_property(instance, "dest_dir", self._result_dir)
        Helper.set_property(instance, "after_nl", "CRLF")
        instance.execute()
        files = glob(os.path.join(self._result_dir, "*.csv"))
        assert 2 == len(files)
        for file in files:
            with open(file, mode="r", newline="") as f:
                line = f.readline()
                assert line.endswith("\r\n")

    def test_execute_ng_not_enough_key(self):
        # create test jsonl
        data = [
            {"id": "123456789", "name": "A", "age": "25"},
            {"id": "1234567890",
             "name": "B",
             "age": "30",
             "value": [{"key": "test_key", "value": 999},
                       {"key": 'test"01"', "value": "true"}]},
        ]
        self._create_jsonl(data, "test_1.json")
        self._create_jsonl(data, "test_2.json")

        # set the essential attributes
        instance = JsonlToCsv()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "src_dir", self._data_dir)
        Helper.set_property(instance, "src_pattern", "test.*.json")
        Helper.set_property(instance, "dest_dir", self._result_dir)
        with pytest.raises(ValueError) as e:
            instance.execute()
        assert "dict contains fields not in fieldnames: 'value'" == str(e.value)
