import os
import shutil

import pytest

from cliboa.conf import env
from cliboa.util.sqlite import SqliteAdapter


class TestSqliteCreation(object):
    def setup_method(self, method):
        self._db_dir = os.path.join(env.BASE_DIR, "db")
        self._adptr = SqliteAdapter()

    def test_connect_ok(self):
        # create spam.db
        os.makedirs(self._db_dir)
        db_file = os.path.join(self._db_dir, "spam.db")
        self._adptr.connect(db_file)
        exists_db_file = os.path.exists(db_file)
        shutil.rmtree(self._db_dir)
        assert exists_db_file is True

    def test_close_ok(self):
        # create spam.db
        os.makedirs(self._db_dir)
        db_file = os.path.join(self._db_dir, "spam.db")
        self._adptr.connect(db_file)
        self._adptr.close()
        exists_db_file = os.path.exists(db_file)
        shutil.rmtree(self._db_dir)
        assert exists_db_file is True

    def test_fetch_ok(self):
        # create spam.db
        os.makedirs(self._db_dir)
        db_file = os.path.join(self._db_dir, "spam.db")
        self._adptr.connect(db_file)
        self._adptr.execute("create table spam_table (id, name, age);")
        cursor1 = self._adptr.fetch("select * from spam_table")
        cursor2 = self._adptr.fetch("select * from spam_table")
        os.path.exists(db_file)
        shutil.rmtree(self._db_dir)
        assert isinstance(type(cursor1), type(cursor2)) is False

    def test_execute_many_insert_ok(self):
        # create spam.db
        os.makedirs(self._db_dir)
        db_file = os.path.join(self._db_dir, "spam.db")
        self._adptr.connect(db_file)
        self._adptr.execute("create table spam_table (id, name, age);")

        def test_insert():
            dummy_data = [{"id": 1, "name": "spam1", "age": 24}]
            self._adptr.execute_many_insert(
                "spam_table", ["id", "name", "age"], dummy_data
            )

        test_insert()
        self._adptr.commit()
        cursor = self._adptr.fetch("select * from spam_table;")
        shutil.rmtree(self._db_dir)
        for c in cursor:
            assert c == (1, "spam1", 24)

    def test_execute_many_insert_ng_no_tblname(self):
        # create spam.db
        os.makedirs(self._db_dir)
        db_file = os.path.join(self._db_dir, "spam.db")
        self._adptr.connect(db_file)
        self._adptr.execute("create table spam_table (id, name, age);")

        def test_insert():
            dummy_data = [{"id": 1, "name": "spam1", "age": 24}]
            self._adptr.execute_many_insert(None, ["id", "name", "age"], dummy_data)

        with pytest.raises(ValueError) as execinfo:
            test_insert()
        shutil.rmtree(self._db_dir)
        assert str(execinfo.value) == "Parameters are missing"

    def test_create_table(self):
        # create spam.db
        os.makedirs(self._db_dir)
        db_file = os.path.join(self._db_dir, "spam.db")
        self._adptr.connect(db_file)

        columns = ["id", "na\"me", "a'ge"]
        self._adptr.create_table("spam_table", columns)
        result = self._adptr.get_column_names("spam_table")

        shutil.rmtree(self._db_dir)
        assert columns == result
