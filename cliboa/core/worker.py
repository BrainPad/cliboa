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
from cliboa.core.factory import StepExecutorFactory
from cliboa.core.scenario_queue import ScenarioQueue
from cliboa.util.constant import StepStatus
from cliboa.util.lisboa_log import LisboaLog


class ScenarioWorker(object):
    """
    Worker for scenario
    """

    def __init__(self, cmd_args):
        """
        Args:
            cmd_args: command line arguments
        """
        self._logger = LisboaLog.get_logger(__name__)
        self._scenario_queue = ScenarioQueue
        self._cmd_args = cmd_args
        self._listeners = []

    def get_scenario_queue_status(self):
        """
        Get current scenario_queue status
        """
        q = self._scenario_queue.step_queue.__class__.__name__
        q_size = self._scenario_queue.step_queue.size()
        return "{} size is {} .".format(q, q_size)

    def _before_scenario(self):
        """
        Notify to registered listener
        """
        for l in self._listeners:
            l.before_scenario(self)

    def _after_scenario(self):
        """
        Notify to registered listener
        """
        for l in self._listeners:
            l.after_scenario(self)

    def register_listeners(self, listener):
        """
        Register multiple listeners to activate
        """
        self._listeners.append(listener)

    def execute_scenario(self):
        self._before_scenario()
        try:
            return self.__execute_steps()
        except Exception as e:
            self._logger.error(e)
            raise e
        finally:
            self._after_scenario()

    def __execute_steps(self):
        """
        Execute steps in scenario.yml
        """
        res = None
        while not self._scenario_queue.step_queue.is_empty():
            strategy = StepExecutorFactory.create(self._scenario_queue.step_queue.pop())
            res = strategy.execute_steps(self._cmd_args)
            if res is None:
                continue
            elif res == StepStatus.SUCCESSFUL_TERMINATION:
                self._logger.info(
                    "Step response [successful termination]. Scenario will be end."
                )
                break
            else:
                self._logger.error("Step response [%s]. Scenario will be end." % res)
                break

        return StepStatus.SUCCESSFUL_TERMINATION if res is None else res
