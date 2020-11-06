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

from cliboa.util.cache import StorageIO


class TestStorageIO(object):
    def setup_method(self, method):
        self.__tmp_valid_cache_file = "/tmp/cliboa_cache_" + str(os.getpid()) + ".tmp"
        self.__tmp_invalid_cache_file = "/tmp/cliboa_cache.tmp"
        if os.path.exists(self.__tmp_valid_cache_file):
            os.remove(self.__tmp_valid_cache_file)

    def test_save_ok(self):
        s = StorageIO()
        s.save(["spam"])
        assert os.path.exists(self.__tmp_valid_cache_file) is True

    def test_save_ng(self):
        s = StorageIO()
        s.save("spam")
        assert os.path.exists(self.__tmp_invalid_cache_file) is False
