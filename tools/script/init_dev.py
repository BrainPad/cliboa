#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
from shutil import copyfile

"""
Check current python version and prepare appropriate Pipfile
"""
py_major_version = "{}.{}".format(sys.version_info.major, sys.version_info.minor)
py_major_version_and_pipfile = {
    "3.7": "Pipfile.above37",
    "3.8": "Pipfile.above38",
    "3.9": "Pipfile.above39",
    "3.10": "Pipfile.above310",
}
py_major_version_and_pyproject_toml = {
    "3.7": "pyproject.above37.toml",
    "3.8": "pyproject.above38.toml",
    "3.9": "pyproject.above39.toml",
    "3.10": "pyproject.above310.toml",
}
pipfile_path = os.path.join("cliboa/template", py_major_version_and_pipfile[py_major_version])
copyfile(pipfile_path, os.path.join("./", "Pipfile"))

pyproject_toml_path = os.path.join(
    "cliboa/template", py_major_version_and_pyproject_toml[py_major_version]
)
copyfile(pyproject_toml_path, os.path.join("./", "pyproject.toml"))
