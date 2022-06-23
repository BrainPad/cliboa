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
from abc import abstractmethod
from multiprocessing import Pool

import cloudpickle
from multiprocessing_logging import install_mp_handler

from cliboa.core.scenario_queue import ScenarioQueue
from cliboa.util.exception import StepExecutionFailed
from cliboa.util.lisboa_log import LisboaLog

__all__ = ["SingleProcExecutor", "MultiProcExecutor", "MultiProcWithConfigExecutor"]


class StepExecutor(object):
    """
    Strategy class when steps in scenario file are executed
    """

    def __init__(self, obj):
        """
        Args:
            q: queue which stores execution target steps
            cmd_args: command line arguments
        """
        self._logger = LisboaLog.get_logger(__name__)
        self._step = obj

    @abstractmethod
    def execute_steps(self):
        """
        Execute steps in scenario file
        """


class SingleProcExecutor(StepExecutor):
    """
    Execute steps in queue with single thread
    """

    def execute_steps(self, args):
        try:
            cls = self._step[0]
            ret = cls.trigger(args)
            return ret

        except Exception as e:
            self._logger.error(
                "Exception occurred during %s execution. Error Message: %s"
                % (cls.__class__.__name__, str(e))
            )
            raise e


class MultiProcExecutor(StepExecutor):
    """
    Execute steps in queue with multi process
    """

    @staticmethod
    def _async_step_execute(cls):
        try:
            clz = cloudpickle.loads(cls)
            clz.trigger()
            return "OK"
        except Exception as e:
            LisboaLog.get_logger(__name__).error(e)
            return "NG"

    def execute_steps(self, args):
        self._logger.info(
            "Multi process start. Execute step count=%s." % ScenarioQueue.step_queue.multi_proc_cnt
        )
        install_mp_handler()
        packed = [cloudpickle.dumps(step) for step in self._step]

        try:
            with Pool(processes=ScenarioQueue.step_queue.multi_proc_cnt) as p:
                for r in p.imap_unordered(self._async_step_execute, packed):
                    if r == "NG":
                        if ScenarioQueue.step_queue.force_continue:
                            self._logger.warning("Multi process response. %s" % r)
                        else:
                            raise StepExecutionFailed("Multi process response. %s" % r)
        except Exception as e:
            self._logger.error("Exception occurred during multi process execution.")
            raise e


class MultiProcWithConfigExecutor(MultiProcExecutor):
    """
    Execute steps in queue in multi process with given config
    """

    def __init__(self, obj):
        super().__init__(obj)
        self._step = obj[0].steps
        self._multi_proc_cnt = None
        if "multi_process_count" in obj[0].config.keys():
            self._multi_proc_cnt = obj[0].config["multi_process_count"]

    def execute_steps(self, args):
        self._logger.info("Multi process start. Execute step count=%s." % self._multi_proc_cnt)
        install_mp_handler()
        packed = [cloudpickle.dumps(step) for step in self._step]

        try:
            with Pool(processes=self._multi_proc_cnt) as p:
                for r in p.imap_unordered(self._async_step_execute, packed):
                    if r == "NG":
                        if ScenarioQueue.step_queue.force_continue:
                            self._logger.warning("Multi process response. %s" % r)
                        else:
                            raise StepExecutionFailed("Multi process response. %s" % r)
        except Exception as e:
            self._logger.error("Exception occurred during multi process execution.")
            raise e
