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
from pprint import pprint

from cliboa.conf import env
from cliboa.scenario.base import BaseSqlite
from cliboa.util.exception import SqliteInvalid
from cliboa.util.helper import Helper

class TestBaseSqlite(object):
    def setup_method(self, method):
        self._db_dir = os.path.join(env.BASE_DIR, "db")

    def test_execute_ng_invalid_dbname(self):
        """
        sqlite db does not exist.
        """
        try:
            instance = BaseSqlite()
            db_file = os.path.join(self._db_dir, "spam.db")
            Helper.set_property(instance, "dbname", db_file)
            Helper.set_property(instance, "tblname", "spam_table")
            instance.execute()
        except Exception as e:
            tb = sys.exc_info()[2]
            assert "not found" in "{0}".format(e.with_traceback(tb))
