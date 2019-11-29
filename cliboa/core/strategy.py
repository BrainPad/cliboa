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

from cliboa.core.scenario_queue import ScenarioQueue
from cliboa.util.lisboa_log import LisboaLog
from cliboa.util.exception import StepExecutionFailed

__all__ = ["SingleProcExecutor", "MultiProcExecutor"]


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

        # enable to regist multiple listeners
        self._listeners = []

    def get_queue_status(self):
        """
        Get current execution queue status
        """
        q_name = self._step[0].__class__.__name__
        return f"{q_name}"

    @abstractmethod
    def execute_steps(self):
        """
        Execute steps in scenario file
        """
        pass

    def regist_listeners(self, listener):
        """
        Regist multiple listeners to activate
        """
        self._listeners.append(listener)

    def _before_step(self):
        """
        Notify to registerd listener
        """
        for l in self._listeners:
            l.before_step(self)

    def _after_step(self):
        """
        Notify to registerd listener
        """
        for l in self._listeners:
            l.after_step(self)

    def _after_completion(self):
        """
        Notify to registerd listener
        """
        for l in self._listeners:
            l.after_completion()


class SingleProcExecutor(StepExecutor):
    """
    Execute steps in queue with single thread
    """

    def execute_steps(self, args):
        try:
            self._before_step()
            cls = self._step[0]
            ret = cls.execute(args)
            self._after_step()
            return ret

        except Exception as e:
            self._logger.error(
                "Exception occurred during %s execution. " % cls.__class__.__name__
            )
            raise StepExecutionFailed(e)

        finally:
            self._after_completion()


class MultiProcExecutor(StepExecutor):
    """
    Execute steps in queue with multi process
    """

    def _async_step_execute(self, cls):
        try:
            self._before_step()
            cls.execute()
            self._after_step()
            return "OK"
        except Exception as e:
            self._logger.error(e)
            return "NG"

    def execute_steps(self, args):
        self._logger.info(
            "Multi process start. Execute step count=%s." % len(self._step)
        )

        try:
            with Pool(processes=ScenarioQueue.step_queue.multi_proc_cnt) as p:
                for r in p.imap_unordered(self._async_step_execute, self._step):
                    if r == "NG":
                        raise StepExecutionFailed("Multi process response. %s" % r)
        except Exception as e:
            self._logger.error("Exception occurred during multi process execution.")
            raise StepExecutionFailed(e)
        finally:
            self._after_completion()
