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
from queue import Queue

import cliboa
from cliboa.core.step_queue import *


class ScenarioQueue(object):
    """
    Composition of extract, transform, load queus
    """

    step_queue = StepQueue()

    class __metaclass__(type):
        @property
        def step_queue(cls):
            return cls.step_queue

        @step_queue.setter
        def step_queue(cls, q):
            cls.step_queue = q
