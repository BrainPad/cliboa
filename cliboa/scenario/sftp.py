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

from pydantic import BaseModel

from cliboa.adapter.sftp import SftpAdapter
from cliboa.scenario.base import BaseStep
from cliboa.util.base import _warn_deprecated, _warn_deprecated_args


class BaseSftp(BaseStep):
    class Arguments(BaseModel):
        src_dir: str
        src_pattern: str
        dest_dir: str = ""
        host: str
        port: int = 22
        user: str
        password: str | None = None
        key: str | None = None
        passphrase: str | None = None
        endfile_suffix: str | None = None
        timeout: int = 30
        retry_count: int = 3

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _src_dir(self):
        return self.args.src_dir

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _src_pattern(self):
        return self.args.src_pattern

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _dest_dir(self):
        return self.args.dest_dir

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _host(self):
        return self.args.host

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _port(self):
        return self.args.port

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _user(self):
        return self.args.user

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _password(self):
        return self.args.password

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _key(self):
        return self.args.key

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _passphrase(self):
        return self.args.passphrase

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _endfile_suffix(self):
        return self.args.endfile_suffix

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _timeout(self):
        return self.args.timeout

    @property
    @_warn_deprecated_args("3.0", "4.0")
    def _retry_count(self):
        return self.args.retry_count

    def get_adapter(self):
        return self._resolve(
            "adapter_sftp",
            SftpAdapter,
            host=self.args.host,
            user=self.args.user,
            password=self.args.password,
            key=self.args.key,
            passphrase=self.args.passphrase,
            timeout=self.args.timeout,
            retryTimes=self.args.retry_count,
            port=self.args.port,
        )

    def get_adaptor(self):
        self.logger.info(
            _warn_deprecated(
                "cliboa.scenario.sftp.BaseSftp.get_adaptor",
                "3.0",
                "4.0",
                "cliboa.scenario.sftp.BaseSftp.get_adapter",
            )
        )
        return self.get_adapter()
