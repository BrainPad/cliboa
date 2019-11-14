import os
import sys
import pytest
import shutil
import sqlite3
from pprint import pprint

from cliboa.conf import env
from cliboa.scenario.load.sqlite import SqliteCreation
from cliboa.util.cache import StorageIO
from cliboa.util.sqlite import SqliteAdapter
from cliboa.util.exception import SqliteInvalid


class TestSqliteCreation(object):
    def setup_method(self, method):
        self.__db_dir = os.path.join(env.BASE_DIR, "db")
        self.__adptr = SqliteAdapter()

    def test_connect_ok(self):
        # create spam.db
        os.makedirs(self.__db_dir)
        db_file = os.path.join(self.__db_dir, "spam.db")
        self.__adptr.connect(db_file)
        exists_db_file = os.path.exists(db_file)
        shutil.rmtree(self.__db_dir)
        assert exists_db_file is True

    def test_close_ok(self):
        # create spam.db
        os.makedirs(self.__db_dir)
        db_file = os.path.join(self.__db_dir, "spam.db")
        self.__adptr.connect(db_file)
        self.__adptr.close()
        exists_db_file = os.path.exists(db_file)
        shutil.rmtree(self.__db_dir)
        assert exists_db_file is True

    def test_fetch_ok(self):
        # create spam.db
        os.makedirs(self.__db_dir)
        db_file = os.path.join(self.__db_dir, "spam.db")
        self.__adptr.connect(db_file)
        self.__adptr.execute("create table spam_table (id, name, age);")
        cursor = self.__adptr.fetch("select * from spam_table")
        exists_db_file = os.path.exists(db_file)
        conn = sqlite3.connect(db_file)
        shutil.rmtree(self.__db_dir)
        assert type(cursor) == type(conn.cursor())

    def test_execute_many_insert_ok(self):
        # create spam.db
        os.makedirs(self.__db_dir)
        db_file = os.path.join(self.__db_dir, "spam.db")
        self.__adptr.connect(db_file)
        self.__adptr.execute("create table spam_table (id, name, age);")

        def test_insert():
            dummy_data = [{"id": 1, "name": "spam1", "age": 24}]
            self.__adptr.execute_many_insert(
                "spam_table", ["id", "name", "age"], dummy_data
            )

        test_insert()
        self.__adptr.commit()
        cursor = self.__adptr.fetch("select * from spam_table;")
        shutil.rmtree(self.__db_dir)
        for c in cursor:
            assert c == (1, "spam1", 24)

    def test_execute_many_insert_ng_no_tblname(self):
        # create spam.db
        os.makedirs(self.__db_dir)
        db_file = os.path.join(self.__db_dir, "spam.db")
        self.__adptr.connect(db_file)
        self.__adptr.execute("create table spam_table (id, name, age);")

        def test_insert():
            dummy_data = [{"id": 1, "name": "spam1", "age": 24}]
            self.__adptr.execute_many_insert(None, ["id", "name", "age"], dummy_data)

        with pytest.raises(ValueError) as execinfo:
            test_insert()
        shutil.rmtree(self.__db_dir)
        assert str(execinfo.value) == "Parameters are missing"
