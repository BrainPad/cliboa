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
from importlib import import_module

from cliboa.core.strategy import MultiProcExecutor, SingleProcExecutor
from cliboa.scenario.base import BaseStep
from cliboa.util.class_util import ClassUtil
from cliboa.util.parallel_with_config import ParallelWithConfig


class StepExecutorFactory:
    """
    Create step execution strategy instance
    """

    @staticmethod
    def create(obj: BaseStep | ParallelWithConfig):
        """
        Args:
            obj: queue which stores execution target steps
        Returns:
            step execution strategy instance
        """
        if isinstance(obj, ParallelWithConfig):
            return MultiProcExecutor(obj)
        else:
            return SingleProcExecutor(obj)


class CustomInstanceFactory(object):
    """
    Import python module and create instance dynamically

    Return:
        Created instance.
        None: If cls_name was not found in the defined class list.
    """

    @staticmethod
    def create(cls_name):
        ret = ClassUtil().describe_class(cls_name)
        if ret is None:
            return None
        (root, mod_name) = ret
        instance = getattr(import_module(root), mod_name)
        return instance()
