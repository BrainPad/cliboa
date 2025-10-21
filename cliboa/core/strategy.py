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
from abc import abstractmethod
from multiprocessing import Pool
from typing import Optional

import cloudpickle
from multiprocessing_logging import install_mp_handler

from cliboa import state
from cliboa.scenario.base import BaseStep
from cliboa.util.base import _BaseObject
from cliboa.util.constant import StepStatus
from cliboa.util.exception import StepExecutionFailed
from cliboa.util.log import _get_logger
from cliboa.util.parallel_with_config import ParallelWithConfig


class _StepExecutor(_BaseObject):
    """
    Strategy class when steps in scenario file are executed
    """

    def __init__(self, obj: BaseStep | ParallelWithConfig):
        super().__init__()
        self._step = obj

    @abstractmethod
    def execute_steps(self) -> Optional[int]:
        """
        Execute steps in scenario file
        """


class SingleProcExecutor(_StepExecutor):
    """
    Execute steps in queue with single thread
    """

    def execute_steps(self, args) -> Optional[int]:
        try:
            state.set(self._step.__class__.__name__)
            return self._step.trigger(args)
        except Exception:
            self._logger.exception(
                "Exception occurred during %s execution." % (self._step.__class__.__name__,)
            )
            return StepStatus.ABNORMAL_TERMINATION


class MultiProcExecutor(_StepExecutor):
    """
    Execute steps in queue with multi process
    """

    @staticmethod
    def _async_step_execute(cls):
        try:
            clz = cloudpickle.loads(cls)
            res = clz.trigger()
            if res == StepStatus.ABNORMAL_TERMINATION:
                return "NG"
            else:
                return "OK"
        except Exception as e:
            _get_logger(__name__).error(e)
            return "NG"

    def execute_steps(self, args) -> Optional[int]:
        # FIXME: args not passed to BaseStep
        state.set("MultiProcess")
        self._logger.info(
            "Multi process start. Execute step count=%s." % self._step.config.multi_process_count
        )
        install_mp_handler()
        packed = [cloudpickle.dumps(step) for step in self._step.steps]

        try:
            with Pool(processes=self._step.config.multi_process_count) as p:
                for r in p.imap_unordered(self._async_step_execute, packed):
                    if r == "NG":
                        if self._step.config.force_continue:
                            self._logger.warning("Multi process response. %s" % r)
                        else:
                            raise StepExecutionFailed("Multi process response. %s" % r)
        except Exception:
            self._logger.exception("Exception occurred during multi process execution.")
            return StepStatus.ABNORMAL_TERMINATION
