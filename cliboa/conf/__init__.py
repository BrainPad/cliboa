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
from importlib import import_module


class Environment:
    """
    setting of cliboa environment values
    """

    def __init__(self):
        """
        Load environment.py in project
        If values are not set in environment.py, default values are used
        """
        env_module = os.environ.get("CLIBOA_ENV")
        if not env_module:
            # Assume executing unit test codes
            mod = import_module("cliboa.common.environment")
        else:
            # Assume executing cfmanager.py
            mod = import_module(env_module)

        for env in dir(mod):
            val = getattr(mod, env)
            setattr(self, env, val)


env = Environment()
