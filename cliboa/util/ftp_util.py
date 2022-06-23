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
from datetime import datetime


class FtpUtil(object):
    def list_files(self, dir, dest, pattern):
        """
        Function that is to download files by regular pattern matching

        Args:
            dir (str): ftp server directory to download
            dest (str): local directory where to place downloaded files from ftp
            pattern (object): regular pattern matching

        Returns (tuple):
            func: list_file_func
            params: parameters for list_file_func

        Raises:
            IOError: ftplib failure
        """
        return (list_file_func, {"dir": dir, "dest": dest, "pattern": pattern})

    def clear_files(self, dir, pattern):
        """
        Remove all files which matche to pattern. Not remove directory.

        Args:
            dir (str): remove target dir
            pattern (object): remove target file pattern

        Returns (tuple):
            func: clear_file_func
            params: parameters for clear_file_func

        Raises:
            IOError: failed to remove
        """
        return (list_file_func, {"dir": dir, "pattern": pattern})

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

    def file_mdtm(self, dir, unixtime=False):
        """
        Returns dictionary of file name and timestamp from target directory

        Args:
            dir (str): directory
            unixtime=False (bool): Whether response time returns
                                   as str(yyyyMMddHHmmss) or unixtime

        Returns (tuple):
            func: file_mdtm_func
            params: parameters for file_mdtm_func

        Returns:
            dict: {file name: update time}


        Raises:
            IOError: ftplib failure
        """
        return (file_mdtm_func, {"dir": dir, "unixtime": unixtime})


def list_file_func(**kwargs):
    files = []
    for src in kwargs["ftp"].nlst(kwargs["dir"]):
        fname = os.path.basename(src)
        if kwargs["pattern"].fullmatch(fname) is None:
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
        if kwargs["pattern"].fullmatch(fname) is None:
            continue
        try:
            kwargs["ftp"].delete(src)
        except Exception:
            # ignore errors. Directory cannot be deleted by ftp#delete.
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
