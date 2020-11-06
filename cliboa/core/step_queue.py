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

__all__ = ["StepQueue"]


class StepQueue(Queue):
    """
    Queue for processing
    """

    _DEFAULT_PARALLEL_CNT = 2

    def __init__(self):
        super().__init__()
        self._multi_proc_cnt = self._DEFAULT_PARALLEL_CNT
        self._force_continue = False

    @property
    def multi_proc_cnt(self):
        return self._multi_proc_cnt

    @multi_proc_cnt.setter
    def multi_proc_cnt(self, multi_proc_cnt):
        self._multi_proc_cnt = multi_proc_cnt

    @property
    def force_continue(self):
        return self._force_continue

    @force_continue.setter
    def force_continue(self, force_continue):
        self._force_continue = force_continue

    def push(self, instance):
        self.put(instance)

    def pop(self):
        return self.get()

    def peek(self):
        """
        Retrieves the next item, but does not remove.
        """
        return self.queue[0]

    def size(self):
        """
        Returns:
            queue size
        """
        return self.qsize()

    def is_empty(self):
        """
        Returns:
            if queue is empty: True
            if queue is not empty: False
        """
        return self.empty()
