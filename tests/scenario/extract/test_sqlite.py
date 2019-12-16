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
import pytest
import sys
import shutil
import sqlite3
from pprint import pprint

from cliboa.conf import env
from cliboa.scenario.base import BaseSqlite
from cliboa.scenario.extract.sqlite import SqliteRead
from cliboa.util.cache import StorageIO
from cliboa.util.lisboa_log import LisboaLog


class TestSqliteRead(object):
    def setup_method(self, method):
        self._db_dir = os.path.join(env.BASE_DIR, "db")
