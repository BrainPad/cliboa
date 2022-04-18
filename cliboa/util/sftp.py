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


class Sftp(object):
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

    def put_file(self, src, dest, endfile_suffix=None):
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
        """
        return (put_file_func, {"src": src, "dest": dest, "endfile_suffix": endfile_suffix})

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
    """
    dirname = os.path.dirname(kwargs["dest"])

    # Create directory if it doesn't exist
    try:
        kwargs["sftp"].stat(dirname)
    except FileNotFoundError:
        kwargs["sftp"].mkdir(dirname)

    tmp_dest = os.path.join(dirname, "." + os.path.basename(kwargs["dest"]))

    _logger = logging.getLogger(__name__)

    if _logger.isEnabledFor(logging.DEBUG):

        def cb(sent, size):
            _logger.debug("Transfer %s / %s" % (sent, size))

        file_size = os.stat(kwargs["src"]).st_size
        with open(kwargs["src"], "rb") as fl:
            _logger.debug("Open src file")
            with kwargs["sftp"].file(tmp_dest, "wb") as fr:
                _logger.debug("Open dest file")
                fr.set_pipelined(True)
                _transfer_with_callback(reader=fl, writer=fr, file_size=file_size, callback=cb)
            _logger.debug("End")
    else:
        kwargs["sftp"].put(kwargs["src"], tmp_dest)

    # Same file name is removed in advance, if exists
    try:
        f = kwargs["sftp"].stat(kwargs["dest"])
        if f:
            kwargs["sftp"].remove(kwargs["dest"])
    except FileNotFoundError:
        pass

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
