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
import sys
import pytest
from pprint import pprint

from cliboa.core.step_queue import StepQueue


class TestScenairoQueue(object):
    def setup_method(self):
        self._scenario_queue = StepQueue()

    def test_push_and_pop(self):
        instance = "spam"
        self._scenario_queue.push(instance)
        ret = self._scenario_queue.pop()
        ret == "spam"

    def test_size(self):
        instance = "spam"
        self._scenario_queue.push(instance)
        size = self._scenario_queue.size()
        # remove test data in queue
        self._scenario_queue.pop()
        assert size == 1

    def test_is_empty(self):
        is_empty = self._scenario_queue.is_empty()
        assert is_empty is True
