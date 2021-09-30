#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
from shutil import copyfile

"""
Check current python version and prepare appropriate Pipfile
"""
py_major_ver = "{}.{}".format(sys.version_info.major, sys.version_info.minor)
py_major_ver_and_pipfile = {
    "3.6": "Pipfile.above36",
    "3.7": "Pipfile.above37",
    "3.8": "Pipfile.above38",
    "3.9": "Pipfile.above39",
}
pipfile_path = os.path.join("cliboa/template", py_major_ver_and_pipfile[py_major_ver])
copyfile(pipfile_path, os.path.join("./", "Pipfile"))
