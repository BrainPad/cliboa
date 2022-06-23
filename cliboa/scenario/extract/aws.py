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

from cliboa.adapter.aws import S3Adapter
from cliboa.scenario.aws import BaseS3
from cliboa.scenario.validator import EssentialParameters
from cliboa.util.cache import ObjectStore
from cliboa.util.constant import StepStatus


class S3Download(BaseS3):
    """
    Download from S3
    """

    def __init__(self):
        super().__init__()
        self._prefix = ""
        self._delimiter = ""
        self._src_pattern = None
        self._dest_dir = "."

    def prefix(self, prefix):
        self._prefix = prefix

    def delimiter(self, delimiter):
        self._delimiter = delimiter

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(self.__class__.__name__, [self._src_pattern])
        valid()

        adapter = S3Adapter(self._access_key, self._secret_key, self._profile)
        client = adapter.get_client()

        p = client.get_paginator("list_objects")
        keys = []
        for page in p.paginate(Bucket=self._bucket, Delimiter=self._delimiter, Prefix=self._prefix):
            for c in page.get("Contents", []):
                path = c.get("Key")
                filename = os.path.basename(path)
                rec = re.compile(self._src_pattern)
                if not rec.fullmatch(filename):
                    continue
                dest_path = os.path.join(self._dest_dir, filename)
                client.download_file(self._bucket, path, dest_path)
                keys.append(path)

        # cache
        ObjectStore.put(self._step, {"bucket": self._bucket, "keys": keys})


class S3DownloadFileDelete(BaseS3):
    """
    Delete all downloaded files from S3
    """

    def __init__(self):
        super().__init__()

    def execute(self, *args):
        stored = ObjectStore.get(self._symbol)
        bucket = stored.get("bucket")
        keys = stored.get("keys")

        if keys is not None and len(keys) > 0:
            self._region = super().get_step_argument("region")
            self._access_key = super().get_step_argument("access_key")
            self._secret_key = super().get_step_argument("secret_key")
            self._profile = super().get_step_argument("profile")
            self._bucket = super().get_step_argument("bucket")
            self._key = super().get_step_argument("key")
            self._prefix = super().get_step_argument("prefix")
            self._delimiter = super().get_step_argument("delimiter")
            self._src_pattern = super().get_step_argument("src_pattern")

            adapter = S3Adapter(self._access_key, self._secret_key, self._profile)
            client = adapter.get_client()

            for key in keys:
                client.delete_object(Bucket=bucket, Key=key)
                self._logger.info("%s is successfully deleted." % bucket + "/" + key)
        else:
            self._logger.info("No files to delete.")


class S3FileExistsCheck(BaseS3):
    """
    File check in S3
    """

    def __init__(self):
        super().__init__()
        self._prefix = ""
        self._delimiter = ""
        self._src_pattern = None

    def prefix(self, prefix):
        self._prefix = prefix

    def delimiter(self, delimiter):
        self._delimiter = delimiter

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(self.__class__.__name__, [self._src_pattern])
        valid()

        adapter = S3Adapter(self._access_key, self._secret_key, self._profile)

        p = adapter.get_client().get_paginator("list_objects")
        for page in p.paginate(Bucket=self._bucket, Delimiter=self._delimiter, Prefix=self._prefix):
            for c in page.get("Contents", []):
                filename = os.path.basename(c.get("Key"))
                rec = re.compile(self._src_pattern)
                if not rec.fullmatch(filename):
                    continue
                # The file exist
                self._logger.info("File was found in S3. After process will be processed")
                return None

        # The file does not exist
        self._logger.info("File not found in S3. After process will not be processed")
        return StepStatus.SUCCESSFUL_TERMINATION
