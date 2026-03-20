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

import pytest

from cliboa.conf import env
from cliboa.scenario.sqlite import BaseSqlite


class TestBaseSqlite:
    def setup_method(self, method):
        self._db_dir = os.path.join(env.BASE_DIR, "db")

    def test_execute_ng_invalid_dbname(self):
        """
        sqlite db does not exist.
        """
        with pytest.raises(Exception):
            instance = BaseSqlite()
            db_file = os.path.join(self._db_dir, "spam.db")
            instance._set_arguments(
                {
                    "dbname": db_file,
                    "tblname": "spam_table",
                }
            )
