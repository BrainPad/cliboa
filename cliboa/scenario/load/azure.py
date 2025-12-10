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
from cliboa.scenario.file import FileRead
from cliboa.util.constant import StepStatus


class AzureBlobUpload(BaseAzureBlob, FileRead):
    """
    Upload to Azure Blob storage
    """

    class Arguments(BaseAzureBlob.Arguments, FileRead.Arguments):
        dest_dir: str
        quit: bool = False

    def execute(self, *args):
        service = BlobServiceAdapter().get_client(
            account_url=self.args.account_url,
            account_access_key=self.args.account_access_key,
            connection_string=self.args.connection_string,
        )
        files = self.get_src_files()

        if len(files) > 0:
            for f in files:
                path = os.path.join(self.args.dest_dir, os.path.basename(f))
                blob_client = service.get_blob_client(container=self.args.container_name, blob=path)
                with open(f, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
        else:
            self.logger.info(
                "Files to upload do not exist. File pattern: {}".format(
                    os.path.join(self.args.src_dir, self.args.src_pattern)
                )
            )
            if self.args.quit is True:
                return StepStatus.SUCCESSFUL_TERMINATION
