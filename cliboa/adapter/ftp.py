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
import errno
from ftplib import FTP, FTP_TLS
from time import sleep

from cliboa.util.lisboa_log import LisboaLog


class FtpAdapter(object):
    """
    Ftp Adaptor
    """

    TIMEOUT_SEC = 30
    RETRY_SEC = 10

    def __init__(
        self,
        host,
        user,
        password,
        timeout=TIMEOUT_SEC,
        retryTimes=3,
        port=21,
        tls=False,
    ):
        """
        Must set whether password or key

        Args:
            host (str): hostname
            user (str): username
            password (str): password
            timeout=30 (int): timeout seconds
            retryTimes=3 (int): retry count
            port=21 (int): port number
            tls=False (bool): use secure connection
        """

        self._host = host
        self._user = user
        self._password = password
        self._timeout = timeout
        self._retryTimes = retryTimes
        self._port = port
        self._tls = tls
        self._logger = LisboaLog.get_logger(__name__)

    def execute(self, obj):
        """
        Execute sftp process.
        Pass a result of cliboa.util.sftp.FtpUtil.{{ function }} for the arguments.

        Args:
            obj (tuple):
                obj[0]: A function to be executed.
                obj[1]: Arguments when function is executed.
        """
        return self._execute(obj[0], obj[1])

    def _execute(self, func, kwargs):

        if not func:
            raise ValueError("Function must not be empty.")

        for _ in range(self._retryTimes):
            try:
                return self._ftp_call(func, **kwargs)
            except Exception as e:
                self._logger.warning(e)
                self._logger.warning(kwargs)

            self._logger.warning("Unexpected error occurred. Retry will start in 10 sec.")
            sleep(FtpAdapter.RETRY_SEC)

        raise IOError(errno.ENOENT, "FTP failed.")

    def _ftp_call(self, func, **kwargs):
        if self._tls:
            with FTP_TLS(host=self._host, timeout=self._timeout) as ftp:
                ftp.set_debuglevel(1)
                ftp.login(user=self._user, passwd=self._password)
                ftp.prot_p()
                return func(ftp=ftp, **kwargs)
        else:
            with FTP() as ftp:
                ftp.set_debuglevel(1)
                ftp.connect(host=self._host, port=self._port, timeout=self._timeout)
                ftp.login(user=self._user, passwd=self._password)
                return func(ftp=ftp, **kwargs)
