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
import os
import stat
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
        Pass a result of cliboa.adapter.sftp.Sftp.{{ function }} for the arguments.

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
                ssh.set_missing_host_key_policy(AutoAddPolicy())  # nosec
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

    def list_files(self, dir, dest, pattern, endfile_suffix=None, ignore_empty_file=False):
        """
        Fetch all the files in specified directory

        Args:
            dir (str): fetch target directory
            dest (str): local directory to save files
            pattern (object): fetch file pattern
            endfile_suffix=None (str): Download a file only if "filename + endfile_suffix" is exists
            ignore_empty_file=False (bool): If True, size zero files are not be downloaded

        Returns (tuple):
            func: list_file_func
            params: parameters for list_file_func

        Raises:
            IOError: failed to get data
        """
        return (
            list_file_func,
            {
                "dir": dir,
                "dest": dest,
                "pattern": pattern,
                "endfile_suffix": endfile_suffix,
                "ignore_empty_file": ignore_empty_file,
            },
        )

    def clear_files(self, dir, pattern):
        """
        Remove all the files which matche to pattern. Not remove directory.

        Args:
            dir (str): remove target dir
            pattern (object): remove target file pattern

        Returns (tuple):
            func: clear_file_func
            params: parameters for clear_file_func

        Raises:
            IOError: failed to remove
        """
        return (clear_file_func, {"dir": dir, "pattern": pattern})

    def remove_specific_file(self, dir, fname):
        """
        Remove specific file (by name) from target directory

        Args:
            dir (str): remove target dir
            fname (str): file name (exact match)

        Returns (tuple):
            func: remove_specific_file_func
            params: parameters for remove_specific_file_func

        Raises:
            IOError: failed to remove
        """
        return (remove_specific_file_func, {"dir": dir, "fname": fname})

    def get_specific_file(self, src, dest):
        """
        Fetch file by exact pattern match

        Args:
            src (str): fetch target absolute path
            dest (str): local directory to save file

        Returns (tuple):
            func: get_specific_file_func
            params: parameters for get_specific_file_func

        Raises:
            IOError: failed to get data
        """
        return (get_specific_file_func, {"src": src, "dest": dest})

    def put_file(self, src, dest, put_intermediation, endfile_suffix=None):
        """
        Upload file to sftp server

        Args:
            src (str): local file to upload
            dest (str): destination sftp path to upload
            endfile_suffix=None (str): Places file with original file name
                            + "endfile_suffix" when upload completed
        Args:
            src (str): fetch target absolute path
            dest (str): local directory to save file

        Returns (tuple):
            func: put_file_func
            params: parameters for put_file_func

        Raises:
            IOError: failed to upload
            :param put_intermediation:
        """
        return (
            put_file_func,
            {
                "src": src,
                "dest": dest,
                "put_intermediation": put_intermediation,
                "endfile_suffix": endfile_suffix,
            },
        )

    def file_exists_check(self, dir, pattern, ignore_empty_file=False):
        """
        Fetch all the files name in specified directory

        Args:
            dir (str): fetch target directory
            pattern (object): fetch file pattern
            ignore_empty_file=False (bool):
             If True, It is treated as if there is no file with size 0.

        Returns (tuple):
            func: file_exists_check_func
            params: parameters for file_exists_check_func

        Raises:
            IOError: failed to get data
        """
        return (
            file_exists_check_func,
            {"dir": dir, "pattern": pattern, "ignore_empty_file": ignore_empty_file},
        )


def list_file_func(**kwargs):
    """
    Get all the files which matches to pattern
    """
    endfile_suffix = kwargs["endfile_suffix"]
    targets = kwargs["sftp"].listdir(kwargs["dir"])
    files = []
    for f in targets:
        if kwargs["pattern"].fullmatch(f) is None:
            continue
        if endfile_suffix and not f + endfile_suffix in targets:
            continue

        fpath = os.path.join(kwargs["dir"], f)
        if _is_file(kwargs["sftp"], fpath):
            if kwargs["ignore_empty_file"] is True:
                size = _get_file_size(kwargs["sftp"], fpath)
                if size == 0:
                    continue
            kwargs["sftp"].get(fpath, os.path.join(kwargs["dest"], f))
            files.append(f)
    return files


def clear_file_func(**kwargs):
    """
    All the files which match to pattern.
    """
    files = []
    for f in kwargs["sftp"].listdir(kwargs["dir"]):
        if kwargs["pattern"].fullmatch(f) is None:
            continue

        fpath = os.path.join(kwargs["dir"], f)
        if _is_file(kwargs["sftp"], fpath):
            kwargs["sftp"].remove(fpath)
            files.append(fpath)
    return files


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
    '.' as prefix is added to the file name
    while uploading and then the prefix '.'
    will be removed (rename the file) when upload complete.
    The function to add and remove the prefix can be disabled
    by specifying "None" for "put_intermediation".
    """
    dirname = os.path.dirname(kwargs["dest"])

    # Create directory if it doesn't exist
    try:
        kwargs["sftp"].stat(dirname)
    except FileNotFoundError:
        kwargs["sftp"].mkdir(dirname)

    # Same file name is removed in advance, if exists
    try:
        f = kwargs["sftp"].stat(kwargs["dest"])
        if f:
            kwargs["sftp"].remove(kwargs["dest"])
    except FileNotFoundError:
        pass

    if kwargs["put_intermediation"] is None:
        kwargs["sftp"].put(kwargs["src"], os.path.join(dirname, os.path.basename(kwargs["dest"])))
    else:
        tmp_dest = os.path.join(
            dirname, kwargs["put_intermediation"] + os.path.basename(kwargs["dest"])
        )
        kwargs["sftp"].put(kwargs["src"], tmp_dest)
        kwargs["sftp"].rename(tmp_dest, kwargs["dest"])

    endfile_suffix = kwargs["endfile_suffix"]
    if endfile_suffix is not None:
        endfile = kwargs["src"] + endfile_suffix
        open(endfile, mode="w").close()
        kwargs["sftp"].put(endfile, kwargs["dest"] + endfile_suffix)
        os.remove(endfile)


def file_exists_check_func(**kwargs):
    """
    Get all the file names which matches to pattern
    """
    files = []
    for f in kwargs["sftp"].listdir(kwargs["dir"]):
        if kwargs["pattern"].fullmatch(f) is None:
            continue

        fpath = os.path.join(kwargs["dir"], f)
        if _is_file(kwargs["sftp"], fpath):
            if kwargs["ignore_empty_file"] is True:
                if _get_file_size(kwargs["sftp"], fpath) == 0:
                    continue
            files.append(f)
    return files


def _transfer_with_callback(reader, writer, file_size, callback):
    """
    For Debug
    """
    size = 0
    _logger = logging.getLogger(__name__)
    _logger.debug("Call back method")
    while True:
        data = reader.read(32768)
        writer.write(data)
        size += len(data)
        if len(data) == 0:
            _logger.debug("No data")
            break
        if callback is not None:
            callback(size, file_size)
    _logger.debug("Size %s" % size)
    return size


def _is_file(sftp, path):
    """
    Returns True if target object is a file, False if not.
    """
    info = sftp.stat(path)
    if stat.S_ISREG(info.st_mode):
        return True
    else:
        return False


def _get_file_size(sftp, path):
    """
    Returns a file size
    """
    info = sftp.stat(path)
    return info.st_size
