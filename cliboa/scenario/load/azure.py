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

from cliboa.adapter.azure import BlobServiceAdapter
from cliboa.scenario.azure import BaseAzureBlob
from cliboa.scenario.validator import EssentialParameters
from cliboa.util.constant import StepStatus


class AzureBlobUpload(BaseAzureBlob):
    """
    Upload to Azure Blob storage
    """

    def __init__(self):
        super().__init__()
        self._src_dir = ""
        self._src_pattern = None
        self._dest_dir = None
        self._quit = False

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def quit(self, quit):
        self._quit = quit

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern, self._dest_dir]
        )
        valid()

        service = BlobServiceAdapter().get_client(
            account_url=self._account_url,
            account_access_key=self._account_access_key,
            connection_string=self._connection_string,
        )
        files = super().get_target_files(self._src_dir, self._src_pattern)

        if len(files) > 0:
            for f in files:
                path = os.path.join(self._dest_dir, os.path.basename(f))
                blob_client = service.get_blob_client(
                    container=self._container_name, blob=path
                )
                with open(f, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
        else:
            self._logger.info(
                "Files to upload do not exist. File pattern: {}".format(
                    os.path.join(self._src_dir, self._src_pattern)
                )
            )
            if self._quit is True:
                return StepStatus.SUCCESSFUL_TERMINATION
