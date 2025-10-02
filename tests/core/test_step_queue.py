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

from cliboa.core.step_queue import StepQueue


class TestStepQueue(object):

    _DUMMY_PARALLEL_CNT = 9

    def setup_method(self):
        self._scenario_queue = StepQueue()

    def test_multi_proc_cnt(self):
        self._scenario_queue.multi_proc_cnt = self._DUMMY_PARALLEL_CNT
        assert self._scenario_queue.multi_proc_cnt == self._DUMMY_PARALLEL_CNT

    def test_push_and_pop(self):
        instance = "spam"
        self._scenario_queue.push(instance)
        ret = self._scenario_queue.pop()
        ret == "spam"

    def test_peek(self):
        instance = "spam"
        self._scenario_queue.push(instance)
        assert self._scenario_queue.peek() == "spam"

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
