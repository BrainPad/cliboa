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
import re

from cliboa.adapter.azure import BlobServiceAdapter
from cliboa.scenario.azure import BaseAzureBlob
from cliboa.scenario.validator import EssentialParameters


class AzureBlobDownload(BaseAzureBlob):
    """
    Download from Azure blob storage
    """

    def __init__(self):
        super().__init__()
        self._prefix = ""
        self._src_pattern = None
        self._dest_dir = None

    def prefix(self, prefix):
        self._prefix = prefix

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(self.__class__.__name__, [self._src_pattern, self._dest_dir])
        valid()

        service = BlobServiceAdapter().get_client(
            account_url=self._account_url,
            account_access_key=self._account_access_key,
            connection_string=self._connection_string,
        )
        container_client = service.get_container_client(self._container_name)
        blobs = container_client.list_blobs(name_starts_with=self._prefix)
        for blob in blobs:
            filename = blob.name
            rec = re.compile(self._src_pattern)
            if not rec.fullmatch(filename):
                continue
            dest_path = os.path.join(self._dest_dir, os.path.basename(filename))
            blob_client = service.get_blob_client(container=self._container_name, blob=filename)

            with open(dest_path, "wb") as local_blob:
                blob_data = blob_client.download_blob()
                blob_data.readinto(local_blob)
