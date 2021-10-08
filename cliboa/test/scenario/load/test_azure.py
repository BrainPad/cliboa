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
from pathlib import Path

from mock import patch

from cliboa.adapter.azure import BlobServiceAdapter
from cliboa.conf import env
from cliboa.scenario.load.azure import AzureBlobUpload
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog


class TestAzureBlobUpload(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    @patch.object(BlobServiceAdapter, "get_client")
    def test_execute_ok(self, m_get_client):
        # Arrange
        service = m_get_client.return_value
        blob_client = service.get_blob_client.return_value
        try:
            os.makedirs(self._data_dir)
            dir_path = Path(self._data_dir)
            (dir_path / "a.txt").touch()
            (dir_path / "b.txt").touch()
            (dir_path / "c.exe").touch()

            # Act
            instance = AzureBlobUpload()
            Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
            # use Postman echo
            Helper.set_property(
                instance, "account_url", "https://testtesttest.blob.core.windows.example/",
            )
            Helper.set_property(instance, "account_access_key", "dummy")
            Helper.set_property(instance, "container_name", "test")
            Helper.set_property(instance, "src_dir", self._data_dir)
            Helper.set_property(instance, "src_pattern", r"(.*)\.txt")
            Helper.set_property(instance, "dest_dir", "out")
            instance.execute()

            # Assert
            assert blob_client.upload_blob.call_count == 2
        finally:
            shutil.rmtree(self._data_dir)
