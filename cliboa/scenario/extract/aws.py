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
        for page in p.paginate(
            Bucket=self._bucket, Delimiter=self._delimiter, Prefix=self._prefix
        ):
            for c in page.get("Contents", []):
                path = c.get("Key")
                filename = os.path.basename(path)
                rec = re.compile(self._src_pattern)
                if not rec.fullmatch(filename):
                    continue
                dest_path = os.path.join(self._dest_dir, filename)
                client.download_file(self._bucket, path, dest_path)
