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

from cliboa.adapter.sftp import SftpAdapter
from cliboa.scenario.sftp import BaseSftp
from cliboa.util.base import _warn_deprecated, _warn_deprecated_args
from cliboa.util.constant import StepStatus


class SftpExtract(BaseSftp):
    def __init__(self):
        super().__init__()
        self.logger.warning(
            _warn_deprecated(
                "cliboa.scenario.load.extract.SftpExtract",
                "3.0",
                "4.0",
                "cliboa.scenario.sftp.BaseSftp",
            )
        )


class SftpDownload(BaseSftp):
    """
    Download files from sftp server
    """

    class Arguments(BaseSftp.Arguments):
        quit: bool = False
        ignore_empty_file: bool = False

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _quit(self):
        return self.args.quit

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _ignore_empty_file(self):
        return self.args.ignore_empty_file

    def execute(self, *args):
        os.makedirs(self.args.dest_dir, exist_ok=True)

        adapter = self.get_adapter()
        obj = adapter.list_files(
            dir=self.args.src_dir,
            dest=self.args.dest_dir,
            pattern=re.compile(self.args.src_pattern),
            endfile_suffix=self.args.endfile_suffix,
            ignore_empty_file=self.args.ignore_empty_file,
        )
        files = adapter.execute(obj)

        if self.args.quit is True and len(files) == 0:
            self.logger.info("No file was found. After process will not be processed")
            return StepStatus.SUCCESSFUL_TERMINATION

        self.logger.info("Files downloaded %s" % files)

        # cache downloaded file names
        self.put_to_context(files)


class SftpDelete(BaseSftp):
    """
    Delete file from sftp server
    """

    def execute(self, *args):
        adapter = self.get_adapter()
        obj = adapter.clear_files(
            dir=self.args.src_dir,
            pattern=re.compile(self.args.src_pattern),
        )
        adapter.execute(obj)


class SftpDownloadFileDelete(BaseSftp):
    """
    Delete all downloaded files.
    """

    Arguments = None

    def execute(self, *args):
        files = self.get_from_context()
        if files is None or len(files) == 0:
            self.logger.info("No files to delete.")
            return

        self.logger.info("Delete files %s" % files)
        adapter = self._resolve(
            "adapter_sftp",
            SftpAdapter,
            host=self.get_symbol_argument("host"),
            user=self.get_symbol_argument("user"),
            password=self.get_symbol_argument("password"),
            key=self.get_symbol_argument("key"),
            passphrase=self.get_symbol_argument("passphrase"),
            timeout=self.get_symbol_argument("timeout"),
            retryTimes=self.get_symbol_argument("retry_count"),
            port=self.get_symbol_argument("port"),
        )
        symbol_src_dir = self.get_symbol_argument("src_dir")
        symbol_endfile_suffix = self.get_symbol_argument("endfile_suffix")

        for file in files:
            obj = adapter.remove_specific_file(
                dir=symbol_src_dir,
                fname=file,
            )
            adapter.execute(obj)
            self.logger.info("%s is successfully deleted." % file)

            if symbol_endfile_suffix:
                obj = adapter.remove_specific_file(
                    dir=symbol_src_dir,
                    fname=file + symbol_endfile_suffix,
                )
                self.logger.info("%s is successfully deleted." % (file + symbol_endfile_suffix))


class SftpFileExistsCheck(BaseSftp):
    """
    File check in sftp server
    """

    class Arguments(BaseSftp.Arguments):
        ignore_empty_file: bool = False

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _ignore_empty_file(self):
        return self.args.ignore_empty_file

    def execute(self, *args):
        adapter = self.get_adapter()
        obj = adapter.file_exists_check(
            dir=self.args.src_dir,
            pattern=re.compile(self.args.src_pattern),
            ignore_empty_file=self.args.ignore_empty_file,
        )
        files = adapter.execute(obj)

        if len(files) == 0:
            self.logger.info("File not found. After process will not be processed")
            return StepStatus.SUCCESSFUL_TERMINATION

        self.logger.info("File was found. After process will be processed")
