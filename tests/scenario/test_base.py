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
from unittest import TestCase

from cliboa.conf import env
from cliboa.scenario.sample_step import SampleCustomStep


class TestBase(TestCase):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    def test_source_path_reader_with_none(self):
        instance = SampleCustomStep()
        ret = instance._source_path_reader(None)

        assert ret is None

    def test_source_path_reader_with_path(self):
        try:
            os.makedirs(self._data_dir, exist_ok=True)
            dummy_pass = os.path.join(self._data_dir, "id_rsa")
            with open(dummy_pass, "w") as f:
                f.write("test")

            instance = SampleCustomStep()

            ret = instance._source_path_reader({"file": dummy_pass})
            assert ret == dummy_pass
            with open(ret, "r") as fp:
                actual = fp.read()
                assert "test" == actual
        finally:
            shutil.rmtree(self._data_dir)

    def test_source_path_reader_with_content(self):
        instance = SampleCustomStep()
        ret = instance._source_path_reader({"content": "test"})
        with open(ret, "r") as fp:
            actual = fp.read()
            assert "test" == actual
