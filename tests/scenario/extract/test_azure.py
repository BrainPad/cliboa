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
from collections import namedtuple
from pathlib import Path

from mock import patch

from cliboa.adapter.azure import BlobServiceAdapter
from cliboa.conf import env
from cliboa.scenario.extract.azure import AzureBlobDownload

Blob = namedtuple("Blob", "name")


class TestAzureBlobDownload(object):
    def setup_method(self, method):
        self._data_dir = os.path.join(env.BASE_DIR, "data")

    @patch.object(BlobServiceAdapter, "get_client")
    def test_execute_ok(self, m_get_client):
        # Arrange
        service = m_get_client.return_value
        container_client = service.get_container_client.return_value
        container_client.list_blobs.return_value = [
            Blob(name="sample/a.txt"),
            Blob(name="sample/b.txt"),
            Blob(name="sample/path/to/x.txt"),
        ]
        blob_client = service.get_blob_client.return_value
        blob_client.download_blob.return_value.readinto = lambda s: s.write(b"foo")

        try:
            # Act
            instance = AzureBlobDownload()
            instance._set_properties(
                {
                    # use Postman echo
                    "account_url": "https://testtesttest.blob.core.windows.example/",
                    "account_access_key": "dummy",
                    "container_name": "test",
                    "prefix": "sample/",
                    "src_pattern": r"(.*)\.txt",
                    "dest_dir": self._data_dir,
                }
            )
            instance.execute()

            # Assert
            dir_path = Path(self._data_dir)
            assert (dir_path / "a.txt").exists()
            assert (dir_path / "b.txt").exists()
            assert (dir_path / "x.txt").exists()
            assert (dir_path / "a.txt").read_bytes() == b"foo"
        finally:
            shutil.rmtree(self._data_dir)
