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
import errno
import os
from datetime import datetime
from ftplib import FTP, FTP_TLS
from time import sleep

from cliboa.util.lisboa_log import LisboaLog


class FtpUtil(object):
    """
    FTP common operation
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

        self.__host = host
        self.__user = user
        self.__password = password
        self.__timeout = timeout
        self.__retryTimes = retryTimes
        self.__port = port
        self.__tls = tls
        self._logger = LisboaLog.get_logger(__name__)

    def list_files(self, dir, dest, pattern):
        """
        Function that is to download files by regular pattern matching

        Args:
            dir (str): ftp server directory to download
            dest (str): local directory where to place downloaded files from ftp
            pattern (object): regular pattern matching

        Returns:
            list: downloaded file names

        Raises:
            IOError: ftplib failure
        """
        return self.__execute(list_file_func, dir=dir, dest=dest, pattern=pattern)

    def clear_files(self, dir, pattern):
        """
        Remove all files which matche to pattern. Not remove directory.

        Args:
            dir (str): remove target dir
            pattern (object): remove target file pattern

        Raises:
            IOError: failed to remove
        """
        self.__execute(clear_file_func, dir=dir, pattern=pattern)

    def remove_specific_file(self, dir, fname):
        """
        Remove specific file (by name) from target directory

        Args:
            dir (str): remove target dir
            fname (str): file name (exact match)

        Raises:
            IOError: failed to remove
        """
        self.__execute(remove_specific_file_func, dir=dir, fname=fname)

    def file_mdtm(self, dir, unixtime=False):
        """
        Returns dictionary of file name and timestamp from target directory

        Args:
            dir (str): directory
            unixtime=False (bool): Whether response time returns
                                   as str(yyyyMMddHHmmss) or unixtime
        Returns:
            dict: {file name: update time}


        Raises:
            IOError: ftplib failure
        """
        return self.__execute(file_mdtm_func, dir=dir, unixtime=unixtime)

    def __execute(self, func, **kwargs):
        """
        Connect to ftp server and execute defined function.

        Args:
            func (fucntion): Callback function after connected to the ftp server.
            kwargs (dict): arguments
                           key name 'ftp' is a reserved word. do NOT use it.

        Raises:
            ValueError: Arguments are empty
            IOError: ftplib failure
        """

        if not func:
            raise ValueError("Function must not be empty.")

        for _ in range(self.__retryTimes):
            try:
                return self.__ftp_call(func, **kwargs)
            except Exception as e:
                self._logger.warning(e)
                self._logger.warning(kwargs)

            self._logger.warning(
                "Unexpected error occurred. Retry will start in 10 sec."
            )
            sleep(FtpUtil.RETRY_SEC)

        raise IOError(errno.ENOENT, "FTP failed.")

    def __ftp_call(self, func, **kwargs):
        if self.__tls:
            with FTP_TLS(host=self.__host, timeout=self.__timeout) as ftp:
                ftp.set_debuglevel(1)
                ftp.login(user=self.__user, passwd=self.__password)
                ftp.prot_p()
                return func(ftp=ftp, **kwargs)
        else:
            with FTP() as ftp:
                ftp.set_debuglevel(1)
                ftp.connect(host=self.__host, port=self.__port, timeout=self.__timeout)
                ftp.login(user=self.__user, passwd=self.__password)
                return func(ftp=ftp, **kwargs)


def list_file_func(**kwargs):
    files = []
    for src in kwargs["ftp"].nlst(kwargs["dir"]):
        fname = os.path.basename(src)
        if kwargs["pattern"].match(fname) is None:
            continue

        os.path.join(kwargs["dir"], src)
        try:
            with open(os.path.join(kwargs["dest"], fname), "wb") as f:
                kwargs["ftp"].retrbinary("RETR " + src, f.write)
            files.append(fname)
        except IOError:
            # ignore error. try to get all files
            pass
    return files


def clear_file_func(**kwargs):
    for src in kwargs["ftp"].nlst(kwargs["dir"]):
        fname = os.path.basename(src)
        if kwargs["pattern"].match(fname) is None:
            continue
        try:
            kwargs["ftp"].delete(src)
        except Exception:
            # ignore erros. Directory cannot be deleted by ftp#delete.
            pass


def remove_specific_file_func(**kwargs):
    kwargs["ftp"].delete(os.path.join(kwargs["dir"], kwargs["fname"]))


def file_mdtm_func(**kwargs):
    res = {}
    for src in kwargs["ftp"].nlst(kwargs["dir"]):
        mdtm = kwargs["ftp"].voidcmd("MDTM %s" % src)[4:].strip()
        if kwargs["unixtime"] is True:
            mdtm = int(datetime.strptime(mdtm, "%Y%m%d%H%M%S").timestamp())
        res[os.path.basename(src)] = mdtm
    return res
