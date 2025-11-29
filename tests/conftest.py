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
import logging
import logging.config
import os
from shutil import copyfile

import pytest

from cliboa.conf import env
from cliboa.util.log import CliboaLogRecord


@pytest.fixture(scope="session", autouse=True)
def setup_cliboa_env(tmpdir_factory):
    """
    Execute once at start test session
    """
    os.makedirs("common", exist_ok=True)
    os.makedirs("conf", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    # copy environment.py
    cmn_env_path = os.path.join("cliboa", "conf", "default_environment.py")
    copyfile(cmn_env_path, os.path.join("common", "environment.py"))

    # copy logging.conf
    conf_path = os.path.join("cliboa", "cli", "template", "logging.conf")
    copyfile(conf_path, os.path.join("conf", "logging.conf"))
    logging.config.fileConfig(env.BASE_DIR + "/conf/logging.conf", disable_existing_loggers=False)
    logging.setLogRecordFactory(CliboaLogRecord)

    yield
