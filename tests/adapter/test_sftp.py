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

from cliboa.adapter.sftp import SftpAdapter, put_file_func


class TestSftpAdapter(object):
    def _adapter(self):
        return SftpAdapter(host="dummy.host", user="dummy_user", password="dummy_pass")

    def test_put_file_default_put_intermediation(self):
        # Backward compatibility: put_intermediation must be optional (#572).
        func, params = self._adapter().put_file(src="local.txt", dest="/remote/local.txt")

        assert func is put_file_func
        assert params["put_intermediation"] == "."
        assert params["endfile_suffix"] is None

    def test_put_file_explicit_put_intermediation(self):
        func, params = self._adapter().put_file(
            src="local.txt",
            dest="/remote/local.txt",
            put_intermediation=None,
            endfile_suffix=".end",
        )

        assert params["put_intermediation"] is None
        assert params["endfile_suffix"] == ".end"
