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

# Directory name which scenario file is placed
SCENARIO_DIR_NAME = "scenario"

# scenario file name excluding extension
SCENARIO_FILE_NAME = "scenario"

# cliboa project directory path
BASE_DIR = os.getcwd()

# Project directory path. Customization is available
PROJECT_DIR = os.path.join(BASE_DIR, "project")

# Common scenario directory path. Customization is available
COMMON_DIR = os.path.join(BASE_DIR, "common")

# Common scenario directory path. Customization is available
COMMON_SCENARIO_DIR = os.path.join(COMMON_DIR, "scenario")

# the blow paths are appended to sys.path of python
SYSTEM_APPEND_PATHS = [COMMON_SCENARIO_DIR]

# common custom classes to make available
COMMON_CUSTOM_CLASSES = []

# project congenital classes to make available
PROJECT_CUSTOM_CLASSES = []
