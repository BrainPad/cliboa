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
import logging
from time import sleep

from paramiko import AutoAddPolicy, SSHClient


class SftpAdapter(object):
    """
    Sftp Adaptor
    """

    TIMEOUT_SEC = 30

    def __init__(
        self,
        host,
        user,
        password=None,
        key=None,
        passphrase=None,
        timeout=TIMEOUT_SEC,
        retryTimes=3,
        port=22,
    ):
        """
        Must set whether password or key

        Args:
            host (str): hostname
            user (str): username
            password (str): password
            key (path): private key path
            passphrase (str): passphrase
            timeout=30 (int): timeout seconds
            retryTimes=3 (int): retry count
            port=22 (int): port number
        Raises:
            ValueError: both password and private key are empty, or both specified.
        """

        logging.getLogger("paramiko").setLevel(logging.CRITICAL)

        if (not password and not key) or (password and key):
            raise ValueError("Illegal paramters are given.")

        self._host = host
        self._user = user
        self._password = password
        self._key = key
        self._passphrase = passphrase
        self._timeout = timeout
        self._retryTimes = retryTimes
        self._port = 22 if port is None else port
        self._logger = logging.getLogger(__name__)

    def execute(self, obj):
        """
        Execute sftp process.
        Pass a result of cliboa.util.sftp.Sftp.{{ function }} for the arguments.

        Args:
            obj (tuple):
                obj[0]: A function to be executed.
                obj[1]: Arguments when function is executed.
        """
        return self._execute(obj[0], obj[1])

    def _execute(self, func, kwargs):

        if not func:
            raise ValueError("Function must not be empty.")

        err = None
        for _ in range(self._retryTimes):
            ssh = None
            sftp = None

            try:
                ssh = SSHClient()
                ssh.set_missing_host_key_policy(AutoAddPolicy())
                if self._password:
                    ssh.connect(
                        hostname=self._host,
                        username=self._user,
                        password=self._password,
                        port=self._port,
                        timeout=self._timeout,
                    )
                else:
                    ssh.connect(
                        hostname=self._host,
                        username=self._user,
                        key_filename=self._key,
                        passphrase=self._passphrase,
                        port=self._port,
                        timeout=self._timeout,
                    )

                sftp = ssh.open_sftp()

                # call the argument function
                ret = func(sftp=sftp, **kwargs)

                return ret

            except Exception as e:
                self._logger.warning(e)
                self._logger.warning(kwargs)
                err = e

            finally:
                if sftp is not None:
                    sftp.close()
                if ssh is not None:
                    ssh.close()

            self._logger.warning("Unexpected error occurred. Retry will start in 10 sec.")
            sleep(10)

        raise IOError(err, "SFTP failed.")
