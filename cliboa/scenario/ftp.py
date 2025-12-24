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

from cliboa.adapter.ftp import FtpAdapter
from cliboa.scenario.base import BaseStep
from cliboa.util.base import _warn_deprecated


class BaseFtp(BaseStep):
    class Arguments(BaseModel):
        src_dir: str
        src_pattern: str
        dest_dir: str = ""
        host: str
        port: int = 21
        user: str
        password: str | None = None
        timeout: int = 30
        retry_count: int = 3
        tls: bool = False

    def get_adapter(self):
        return self._resolve(
            "adapter_ftp",
            FtpAdapter,
            host=self.args.host,
            user=self.args.user,
            password=self.args.password,
            timeout=self.args.timeout,
            retryTimes=self.args.retry_count,
            port=self.args.port,
            tls=self.args.tls,
        )

    def get_adaptor(self):
        self.logger.info(
            _warn_deprecated(
                "cliboa.scenario.ftp.BaseFtp.get_adaptor",
                "3.0",
                "4.0",
                "cliboa.scenario.ftp.BaseFtp.get_adapter",
            )
        )
        return self.get_adapter()
