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

from cliboa.adapter.file import File
from cliboa.adapter.sftp import SftpAdapter
from cliboa.scenario.sftp import BaseSftp
from cliboa.util.base import _warn_deprecated, _warn_deprecated_args
from cliboa.util.constant import StepStatus


class SftpBaseLoad(BaseSftp):
    def __init__(self):
        super().__init__()
        self.logger.warning(
            _warn_deprecated(
                "cliboa.scenario.load.sftp.SftpBaseLoad",
                "3.0",
                "4.0",
                "cliboa.scenario.sftp.BaseSftp",
            )
        )


class SftpUpload(BaseSftp):
    """
    Upload file to sftp server
    """

    class Arguments(BaseSftp.Arguments):
        dest_dir: str
        quit: bool = False
        ignore_empty_file: bool = False
        put_intermediation: str = "."

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _quit(self):
        return self.args.quit

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _ignore_empty_file(self):
        return self.args.ignore_empty_file

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _put_intermediation(self):
        return self.args.put_intermediation

    def execute(self, *args):
        files = self._resolve("adapter_file", File).get_target_files(
            self.args.src_dir, self.args.src_pattern
        )
        adaptor = self.get_adapter()
        if len(files) > 0:
            for file in files:
                if self.args.ignore_empty_file and os.path.getsize(file) == 0:
                    self.logger.info("0 byte file will no be uploaded %s." % file)
                    continue

                obj = SftpAdapter(
                    host=self.args.host,
                    user=self.args.user,
                    password=self.args.password,
                    key=self.args.key,
                ).put_file(
                    src=file,
                    dest=os.path.join(self.args.dest_dir, os.path.basename(file)),
                    put_intermediation=self.args.put_intermediation,
                    endfile_suffix=self.args.endfile_suffix,
                )
                adaptor.execute(obj)
                self.logger.info("%s is successfully uploaded." % file)
        else:
            self.logger.info(
                "Files to upload do not exist. File pattern: {}".format(
                    os.path.join(self.args.src_dir, self.args.src_pattern)
                )
            )
            if self.args.quit is True:
                return StepStatus.SUCCESSFUL_TERMINATION
