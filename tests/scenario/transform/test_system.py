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
import subprocess
from glob import glob

from cliboa.conf import env
from cliboa.scenario.transform.system import ExecuteShellScript
from tests import BaseCliboaTest


class TestExecuteShellScript(BaseCliboaTest):
    def setUp(self):
        self._data_dir = os.path.join(env.BASE_DIR, "data")
        os.makedirs(self._data_dir, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self._data_dir, ignore_errors=True)

    def test_inline_script(self):
        instance = ExecuteShellScript()
        instance._set_arguments(
            {
                "command": {"content": "touch foo.csv && touch test.csv"},
                "work_dir": self._data_dir,
            }
        )

        instance.execute()

        files = glob(os.path.join(self._data_dir, "*.csv"))
        file_names = list(map(lambda x: os.path.basename(x), files))

        assert len(files) == 2
        assert "foo.csv" in file_names
        assert "test.csv" in file_names

    def test_file_script(self):
        test_script_path = os.path.join(self._data_dir, "test.sh")
        test_file_path = os.path.join(self._data_dir, "foo.csv")

        with open(test_script_path, "w") as t:
            t.write("#!/bin/sh\n")
            t.write("\n")
            t.write("touch {}".format(test_file_path))

        subprocess.call(["chmod", "0777", test_script_path])

        instance = ExecuteShellScript()
        instance._set_arguments({"command": {"file": test_script_path}})
        instance.execute()

        files = glob(os.path.join(self._data_dir, "*.csv"))

        assert 1 == len(files)
        assert "foo.csv" == os.path.basename(files[0])
