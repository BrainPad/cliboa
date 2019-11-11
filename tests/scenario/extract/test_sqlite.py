#
# Copyright 2019 BrainPad Inc. All Rights Reserved.
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
import sys
import pytest
import shutil
import sqlite3
from pprint import pprint

from cliboa.conf import env
from cliboa.scenario.base import BaseSqlite
from cliboa.scenario.extract.sqlite import SqliteRead
from cliboa.util.cache import StorageIO


class TestSqliteRead(object):
    def setup_method(self, method):
        self._db_dir = os.path.join(env.BASE_DIR, "db")

    def test_execute_ok_select_all(self):
        conn = None
        try:
            # create test db and insert dummy data
            os.makedirs(self._db_dir)
            db_file = os.path.join(self._db_dir, "spam.db")
            conn = sqlite3.connect(db_file)
            conn.execute("create table spam_table (id, name, age);")
            conn.execute("insert into spam_table (id, name, age) values(1,1,1);")
            conn.commit()

            instance = SqliteRead()
            setattr(instance, "dbname", db_file)
            setattr(instance, "tblname", "spam_table")
            setattr(instance, "io", "input")
            instance.execute()
        finally:
            shutil.rmtree(self._db_dir)
            conn.close()

        s = StorageIO()
        fetch_result = []
        with open(s.cache_file, "r", encoding="utf-8") as i:
            fetch_result.append(i.readline())

        s.remove()
        assert fetch_result == ["{'id': 1, 'name': 1, 'age': 1}\n"]

    def test_execute_ok_specific_columns(self):
        conn = None
        try:
            # create test db and insert dummy data
            os.makedirs(self._db_dir)
            db_file = os.path.join(self._db_dir, "spam.db")
            conn = sqlite3.connect(db_file)
            conn.execute("create table spam_table (id, name, age);")
            conn.execute("insert into spam_table (id, name, age) values(1,1,1);")
            conn.commit()

            instance = SqliteRead()
            setattr(instance, "dbname", db_file)
            setattr(instance, "tblname", "spam_table")
            setattr(instance, "columns", ["id"])
            setattr(instance, "io", "input")
            instance.execute()
        finally:
            shutil.rmtree(self._db_dir)
            conn.close()

        s = StorageIO()
        fetch_result = []
        with open(s.cache_file, "r", encoding="utf-8") as i:
            fetch_result.append(i.readline())

        s.remove()
        assert fetch_result == ["{'id': 1}\n"]

    def test_execute_ok_raw_query(self):
        conn = None
        try:
            # create test db and insert dummy data
            os.makedirs(self._db_dir)
            db_file = os.path.join(self._db_dir, "spam.db")
            conn = sqlite3.connect(db_file)
            conn.execute("create table spam_table (id, name, age);")
            conn.execute("insert into spam_table (id, name, age) values(1,1,1);")
            conn.commit()

            instance = SqliteRead()
            setattr(instance, "dbname", db_file)
            setattr(instance, "tblname", "spam_table")
            setattr(instance, "raw_query", "select name from spam_table")
            setattr(instance, "io", "input")
            instance.execute()
        finally:
            shutil.rmtree(self._db_dir)
            conn.close()

        s = StorageIO()
        fetch_result = []
        with open(s.cache_file, "r", encoding="utf-8") as i:
            fetch_result.append(i.readline())

        s.remove()
        assert fetch_result == ["{'name': 1}\n"]
