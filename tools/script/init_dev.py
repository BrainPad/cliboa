#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
from shutil import copyfile

import cliboa

"""
Check current python version and prepare appropriate Pipfile
"""
py_major_ver = "{}.{}".format(sys.version_info.major, sys.version_info.minor)
py_major_ver_and_pipfile = {
    "3.5": "Pipfile.above35",
    "3.6": "Pipfile.above36",
    "3.7": "Pipfile.above37",
}
cliboa_install_path = os.path.dirname(cliboa.__path__[0])
pipfile_path = os.path.join(
    cliboa_install_path, "cliboa/template", py_major_ver_and_pipfile[py_major_ver],
)
copyfile(pipfile_path, os.path.join("./", "Pipfile"))
