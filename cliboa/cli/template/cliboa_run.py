#!/usr/bin/env python3
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
# -*- coding: utf-8 -*-

import logging
import os
import sys

if __name__ == "__main__":
    # setting of environment values
    sys.path.append(os.getcwd())
    os.environ.setdefault("CLIBOA_ENV", "app.cliboa_environment")
    try:
        from cliboa.interface import run

        run()
    except Exception:
        logging.exception("Caught Exception in root application runner.")
        exit(1)
