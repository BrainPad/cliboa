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
from cliboa.scenario.load.sqlite import SqliteCreation
from cliboa.util.cache import StorageIO
from cliboa.util.exception import SqliteInvalid
from cliboa.util.lisboa_log import LisboaLog


class TestSqliteCreation(object):
    def setup_method(self, method):
        self._db_dir = os.path.join(env.BASE_DIR, "db")

    def test_execute_ng_table_not_found(self):
        conn = None
        with pytest.raises(SqliteInvalid) as excinfo:
            # create test db and insert dummy data
            os.makedirs(self._db_dir)
            db_file = os.path.join(self._db_dir, "spam.db")
            conn = sqlite3.connect(db_file)

            # set the essential attributes
            instance = SqliteCreation()
            instance.logger = LisboaLog.get_logger(__name__)
            setattr(instance, "dbname", db_file)
            setattr(instance, "tblname", "spam_table")
            setattr(instance, "io", "output")
            instance.execute()
        shutil.rmtree(self._db_dir)
        conn.close()
        assert "not found" in str(excinfo.value)

    def test_execute_ok_input_data(self):
        conn = None
        o = None
        fetch_result = None
        try:
            # create test db and insert dummy data
            os.makedirs(self._db_dir)
            db_file = os.path.join(self._db_dir, "spam.db")
            conn = sqlite3.connect(db_file)
            conn.execute("create table spam_table (id, name, age);")

            # create dummy input data
            s = StorageIO()
            s.save({"id": 1, "name": 1, "age": 1})

            instance = SqliteCreation()
            instance.logger = LisboaLog.get_logger(__name__)
            setattr(instance, "dbname", db_file)
            setattr(instance, "tblname", "spam_table")
            setattr(instance, "io", "output")
            instance.execute()

            cur = conn.execute("select id, name, age from spam_table;")
            fetch_result = cur.fetchall()
        finally:
            shutil.rmtree(self._db_dir)
            conn.close()
        assert fetch_result == [(1, 1, 1)]

    def test_execute_ok_refresh_with_primkey(self):
        conn = None
        o = None
        fetch_result = None
        try:
            # create test db and insert dummy data
            os.makedirs(self._db_dir)
            db_file = os.path.join(self._db_dir, "spam.db")
            conn = sqlite3.connect(db_file)
            conn.execute("create table spam_table (id, name, age);")

            # create dummy input data
            o = StorageIO()
            o.save({"id": 1, "name": 1, "age": 1})

            instance = SqliteCreation()
            instance.logger = LisboaLog.get_logger(__name__)
            setattr(instance, "dbname", db_file)
            setattr(instance, "tblname", "spam_table")
            setattr(instance, "io", "output")
            setattr(instance, "refresh", True)
            instance.execute()

            cur = conn.execute("select id, name, age from spam_table;")
            fetch_result = cur.fetchall()
        finally:
            shutil.rmtree(self._db_dir)
            conn.close()
        assert fetch_result == [("1", "1", "1")]

    def test_execute_ok_refresh_with_no_primkey(self):
        conn = None
        o = None
        fetch_result = None
        try:
            # create test db and insert dummy data
            os.makedirs(self._db_dir)
            db_file = os.path.join(self._db_dir, "spam.db")
            conn = sqlite3.connect(db_file)
            conn.execute("create table spam_table (id, name, age);")

            # create dummy input data
            s = StorageIO()
            s.save({"id": 1, "name": 1, "age": 1})

            instance = SqliteCreation()
            instance.logger = LisboaLog.get_logger(__name__)
            setattr(instance, "dbname", db_file)
            setattr(instance, "tblname", "spam_table")
            setattr(instance, "io", "output")
            setattr(instance, "refresh", True)
            setattr(instance, "primary_key", "id")
            instance.execute()

            cur = conn.execute("select id, name, age from spam_table;")
            fetch_result = cur.fetchall()
        finally:
            shutil.rmtree(self._db_dir)
            conn.close()
        assert fetch_result == [("1", "1", "1")]
