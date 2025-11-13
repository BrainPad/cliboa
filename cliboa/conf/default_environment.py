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

##################################################
# 1. File/Directory - used within cliboa.interface.run
##################################################

# INTERNAL: app base dir. You can remove BASE_DIR if you want.
BASE_DIR = os.getcwd()

# OPTIONAL: scenario file name excluding extension
SCENARIO_FILE_NAME = "scenario"

# OPTIONAL: scenario yaml file ext
SCENARIO_YAML_EXT = ".yml"

# OPTIONAL: Project directory path.
PROJECT_DIR = os.path.join(BASE_DIR, "project")

# Common scenario directory path.
COMMON_DIR = os.path.join(BASE_DIR, "common")

##################################################
# 2. Logging
##################################################

# OPTIONAL: logging config file used for logging.config.fileConfig within cliboa.interface.run.
# This setting takes precedence over {BASE_DIR}/conf/logging.conf if defined.
LOGGING_CONFIG_PATH = os.path.join(BASE_DIR, "conf", "logging.conf")

# OPTIONAL: logging config dict used for logging.config.dictConfig within cliboa.interface.run.
# This setting takes precedence over LOGGING_CONFIG_PATH if defined.
# LOGGING_CONFIG_DICT = {}

# OPTIONAL: logging mask re pattern.
# If not defined (commented out), the default pattern is applied.
# LOGGING_MASK = ".*password.*|.*secret_key.*"

# OPTIONAL: logging partial mask re pattern.
# If not defined (commented out), the default pattern is applied.
# LOGGING_PARTIAL_MASK = ".*access_key.*|.*token.*"

# OPTIONAL: logging partial mask config.
# expose the first and last LOGGING_PARTIAL_NUM characters.
# If not defined (commented out), the default value is applied.
# LOGGING_PARTIAL_NUM = 3

##################################################
# 3. Scenario class - used for import custom classes
##################################################

# REQUIRED: common custom classes's root module paths.
# remove "cliboa.scenario" and add your custom classes's module root paths.
COMMON_CUSTOM_ROOT_PATHS = ["cliboa.scenario"]

# REQUIRED: common custom classes's root module paths.
PROJECT_CUSTOM_ROOT_PATHS = ["project"]

# REQUIRED: Directory name which scenario python file is placed,
# such as f"{project_custom_root_path}/{project_name}/{SCENARIO_DIR_NAME}/project_step.py".
# If commented out, f"project/{project_name}/project_step.py"
# v2 backward compatibility: Alternate use SCENARIO_DIR_NAME
#                            if PROJECT_SCENARIO_DIR_NAME is not defined.
PROJECT_SCENARIO_DIR_NAME = "scenario"

# REQUIRED: common custom classes to make available.
# You can define empty list this value if you do not use common custom classes.
COMMON_CUSTOM_CLASSES = ["sample_step.SampleStep", "sample_step.SampleStepSub"]

# REQUIRED: project congenital classes to make available.
# You can define empty list this value if you do not use project custom classes.
PROJECT_CUSTOM_CLASSES = []
