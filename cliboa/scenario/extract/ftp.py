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

from cliboa.adapter.ftp import FtpAdapter
from cliboa.scenario.ftp import BaseFtp
from cliboa.util.base import _warn_deprecated
from cliboa.util.constant import StepStatus


class FtpExtract(BaseFtp):
    def __init__(self):
        super().__init__()
        self.logger.warning(
            _warn_deprecated(
                "cliboa.scenario.extract.ftp.FtpExtract",
                "3.0",
                "4.0",
                "cliboa.scenario.ftp.BaseFtp",
            )
        )


class FtpDownload(BaseFtp):
    """
    Download files from ftp server
    """

    class Arguments(BaseFtp.Arguments):
        dest_dir: str
        quit: bool = False

    def execute(self, *args):
        os.makedirs(self.args.dest_dir, exist_ok=True)

        adapter = self.get_adapter()
        obj = adapter.list_files(
            dir=self.args.src_dir,
            dest=self.args.dest_dir,
            pattern=re.compile(self.args.src_pattern),
        )
        files = adapter.execute(obj)

        if self.args.quit is True and len(files) == 0:
            self.logger.info("No file was found. After process will not be processed")
            return StepStatus.SUCCESSFUL_TERMINATION

        # cache downloaded file names
        self.put_to_context(files)


class FtpDownloadFileDelete(FtpExtract):
    """
    Delete all downloaded files.
    """

    def execute(self, *args):
        files = self.get_from_context()

        if files is None or len(files) == 0:
            self.logger.info("No files to delete.")
            return

        self.logger.info("Delete files %s" % files)

        adapter = self._resolve(
            "adapter_ftp",
            FtpAdapter,
            host=self.get_step_argument("host"),
            user=self.get_step_argument("user"),
            password=self.get_step_argument("password"),
            timeout=self.get_step_argument("timeout"),
            retryTimes=self.get_step_argument("retry_count"),
            port=self.get_step_argument("port"),
            tls=self.get_step_argument("tls"),
        )

        for file in files:
            obj = adapter.remove_specific_file(
                dir=self.get_step_argument("src_dir"),
                fname=file,
            )
            adapter.execute(obj)
            self.logger.info("%s is successfully deleted." % file)
