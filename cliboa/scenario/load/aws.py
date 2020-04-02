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

from cliboa.scenario.aws import BaseS3
from cliboa.scenario.validator import EssentialParameters
from cliboa.util.constant import StepStatus


class S3Upload(BaseS3):
    """
    Upload to S3
    """

    def __init__(self):
        super().__init__()
        self._key = None
        self._src_dir = None
        self._src_pattern = None
        self._quit = False

    def key(self, key):
        self._key = key

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def quit(self, quit):
        self._quit = quit

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(
            self.__class__.__name__, [self._src_dir, self._src_pattern]
        )
        valid()

        resource = self._s3_resource()
        bucket = resource.Bucket(self._bucket)
        files = super().get_target_files(self._src_dir, self._src_pattern)

        if len(files) > 0:
            for f in files:
                bucket.upload_file(
                    Key=os.path.join(self._key, os.path.basename(f)), Filename=f
                )
        else:
            self._logger.info(
                "Files to upload do not exist. File pattern: {}".format(
                    os.path.join(self._src_dir, self._src_pattern)
                )
            )
            if self._quit is True:
                return StepStatus.SUCCESSFUL_TERMINATION
