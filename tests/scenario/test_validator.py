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
import shutil
import sqlite3

import pytest

from cliboa.conf import env
from cliboa.scenario.validator import EssentialParameters, SqliteTableExistence
from cliboa.util.exception import CliboaException, SqliteInvalid


class TestEssentialParameters(object):
    def test_essential_parameters_ng(self):
        """
        EssentialParameters invalid case
        """
        with pytest.raises(CliboaException) as excinfo:
            valid = EssentialParameters("DummyClass", [""])
            valid()
        assert "is not specified" in str(excinfo.value)


class TestSqliteTableExistence(object):
    def setup_method(self, method):
        self._db_dir = os.path.join(env.BASE_DIR, "db")

    def test_table_existence_ng_with_exc(self):
        """
        SqliteTableExistende invalid case
        """
        # create test db and insert dummy data
        os.makedirs(self._db_dir)
        db_file = os.path.join(self._db_dir, "spam.db")
        conn = sqlite3.connect(db_file)
        conn.execute("create table spam_table (id, name, age);")
        conn.execute("insert into spam_table (id, name, age) values(1,1,1);")
        conn.commit()
        conn.close()

        with pytest.raises(SqliteInvalid) as excinfo:
            valid = SqliteTableExistence(db_file, "spam_table2")
            valid()
        shutil.rmtree(self._db_dir)
        assert "not found" in str(excinfo.value)

    def test_table_existence_ng_with_bool(self):
        """
        SqliteTableExistence invalid case
        """
        # create test db and insert dummy data
        os.makedirs(self._db_dir)
        db_file = os.path.join(self._db_dir, "spam.db")
        conn = sqlite3.connect(db_file)
        conn.execute("create table spam_table (id, name, age);")
        conn.execute("insert into spam_table (id, name, age) values(1,1,1);")
        conn.commit()
        conn.close()

        valid = SqliteTableExistence(db_file, "spam_table2", True)
        exists_tbl = valid()
        shutil.rmtree(self._db_dir)
        assert exists_tbl is False

    def test_table_existence_ok_with_bool(self):
        """
        SqliteTableExistence invalid case
        """
        # create test db and insert dummy data
        os.makedirs(self._db_dir)
        db_file = os.path.join(self._db_dir, "spam.db")
        conn = sqlite3.connect(db_file)
        conn.execute("create table spam_table (id, name, age);")
        conn.execute("insert into spam_table (id, name, age) values(1,1,1);")
        conn.commit()
        conn.close()

        valid = SqliteTableExistence(db_file, "spam_table", True)
        exists_tbl = valid()
        shutil.rmtree(self._db_dir)
        assert exists_tbl is True
