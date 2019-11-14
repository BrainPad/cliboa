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

from cliboa.util.lisboa_log import LisboaLog


class BaseListener(object):
    """
    Base listener for all the listener classes
    """

    def __init__(self):
        self._logger = LisboaLog.get_logger(__name__)


class ScenarioListener(BaseListener):
    """
    Listener for scenario
    """

    @abstractmethod
    def before_scenario(self, worker):
        """
        Update scenario execution status before scenario execution
        """
        pass

    @abstractmethod
    def after_scenario(self, worker):
        """
        Update scenario execution status after scenario completion
        """
        pass


class StepListener(BaseListener):
    """
    Listener for step
    """

    @abstractmethod
    def before_step(self, strategy):
        """
        Update step status before step execution
        """
        pass

    @abstractmethod
    def after_step(self, strategy):
        """
        Update step status after step
        """
        pass

    @abstractmethod
    def after_completion(self):
        """
        Update step status after step completion
        """
        pass


class ScenarioStatusListener(BaseListener):
    """
    Listener for scenario execution status
    """

    def before_scenario(self, worker):
        self._logger.info(
            "Start scenario execution. %s" % (worker.get_scenario_queue_status())
        )

    def after_scenario(self, worker):
        self._logger.info(
            "Finish scenario execution. %s" % (worker.get_scenario_queue_status())
        )


class StepStatusListener(BaseListener):
    """
    Listener for step execution status
    """

    def before_step(self, strategy):
        self._logger.info("Start step execution in %s." % strategy.get_queue_status())

    def after_step(self, strategy):
        self._logger.info("Finish step execution in %s." % strategy.get_queue_status())

    def after_completion(self):
        self._logger.info("Complete step execution.")
