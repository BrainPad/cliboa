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
from shutil import copyfile
from unittest import TestCase

import pytest


class BaseCliboaTest(TestCase):
    """
    Base class for all test codes
    """

    @pytest.fixture(autouse=True)
    def setup_resource(self):
        """
        setup resource for test
        """
        os.makedirs("common", exist_ok=True)
        os.makedirs("conf", exist_ok=True)
        os.makedirs("logs", exist_ok=True)

        # copy environment.py
        cmn_env_path = os.path.join("cliboa", "common", "environment.py")
        copyfile(cmn_env_path, os.path.join("common", "environment.py"))

        # copy logging.conf
        conf_path = os.path.join("cliboa", "conf", "logging.conf")
        copyfile(conf_path, os.path.join("conf", "logging.conf"))

        # copy cliboa.ini
        conf_path = os.path.join("cliboa", "conf", "cliboa.ini")
        copyfile(conf_path, os.path.join("conf", "cliboa.ini"))

        yield "test in progress"
