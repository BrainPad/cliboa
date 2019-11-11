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
import errno
import logging
from paramiko import SSHClient, AutoAddPolicy
from time import sleep


class Sftp(object):
    """
    SFTP common operation
    """

    TIMEOUT_SEC = 30

    def __init__(
        self,
        host,
        user,
        password=None,
        key=None,
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
        self._timeout = timeout
        self._retryTimes = retryTimes
        self._port = 22 if port is None else port
        self._logger = logging.getLogger(__name__)

    def list_files(self, dir, dest, pattern):
        """
        Fetch all the files in specified directory

        Args:
            dir (str): fetch target directory
            dest (str): local directory to save files
            pattern (object): fetch file pattern

        Returns:
            list: downloaded file names

        Raises:
            IOError: failed to get data
        """
        return self.__execute(list_file_func, dir=dir, dest=dest, pattern=pattern)

    def clear_files(self, dir, pattern):
        """
        Remove all the files which matche to pattern. Not remove directory.

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

    def get_specific_file(self, src, dest):
        """
        Fetch file by exact pattern match

        Args:
            src (str): fetch target absolute path
            dest (str): local directory to save file

        Raises:
            IOError: failed to get data
        """
        self.__execute(get_specific_file_func, src=src, dest=dest)

    def put_file(self, src, dest):
        """
        Upload file to sftp server

        Args:
            src (str): loacal file to upload
            dest (str): destination sftp path to upload

        Raises:
            IOError: failed to upload
        """
        self.__execute(put_file_func, src=src, dest=dest)

    def __execute(self, func, **kwargs):
        """
        Do processing after connected to sftp server

        Args:
            func (fucntion): function which processing is written.
            kwargs (dict): arguments when function is executed

        Raises:
            ValueError: argumets are empty
            IOError: failed to get data
        """

        if not func:
            raise ValueError("Function must not be empty.")

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

            finally:
                if sftp is not None:
                    sftp.close()
                if ssh is not None:
                    ssh.close()

            self._logger.warning(
                "Unexpected error occurred. Retry will start in 10 sec."
            )
            sleep(10)

        raise IOError(errno.ENOENT, "SFTP failed.")


def list_file_func(**kwargs):
    """
    Get all the files which matches to pattern
    """
    files = []
    for f in kwargs["sftp"].listdir(kwargs["dir"]):
        if kwargs["pattern"].match(f) is None:
            continue

        fPath = os.path.join(kwargs["dir"], f)
        try:
            kwargs["sftp"].get(fPath, os.path.join(kwargs["dest"], f))
            files.append(f)
        except IOError:
            # If error occurred, get all the files which matche to pattern
            pass
    return files


def clear_file_func(**kwargs):
    """
    All the files which match to pattenr.
    """
    for f in kwargs["sftp"].listdir(kwargs["dir"]):
        if kwargs["pattern"].match(f) is None:
            continue
        try:
            kwargs["sftp"].remove(os.path.join(kwargs["dir"], f))
        except IOError:
            # Neglect because IOError occurs when pattern maches to directory.
            pass


def remove_specific_file_func(**kwargs):
    """
    Remove specific file (by name) from target directory
    """
    kwargs["sftp"].remove(os.path.join(kwargs["dir"], kwargs["fname"]))


def get_specific_file_func(**kwargs):
    """
    Get only one files which matches to pattern
    """
    kwargs["sftp"].get(kwargs["src"], kwargs["dest"])


def put_file_func(**kwargs):
    """
    Function that is to upload a file.
    '.' as prefix is added to the file name while uploading and then the prefix '.' will be removed (rename the file) when upload complete.
    """
    tmp_dest = os.path.join(
        os.path.dirname(kwargs["dest"]), "." + os.path.basename(kwargs["dest"])
    )
    kwargs["sftp"].put(kwargs["src"], tmp_dest)
    # Same file name is removed in advance, if exists
    try:
        f = kwargs["sftp"].stat(kwargs["dest"])
        if f:
            kwargs["sftp"].remove(kwargs["dest"])
    except FileNotFoundError:
        pass

    kwargs["sftp"].rename(tmp_dest, kwargs["dest"])
