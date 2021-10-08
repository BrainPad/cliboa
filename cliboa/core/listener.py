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
        Execute before scenario start.
        """

    @abstractmethod
    def after_scenario(self, worker):
        """
        Execute after scenario was finished.
        """


class StepListener(BaseListener):
    """
    If you would like to add an extra action for a step,
    create a custom listener class with extend this class,
    and implement any methods below.
    These are called when
    1. before a step is called.
    2. after a step is completed, or when error occured while executing the step.
    3. Very end of the step.
    """

    @abstractmethod
    def before_step(self, *args, **kwargs):
        """
        Execute before a step is called.
        """

    @abstractmethod
    def after_step(self, *args, **kwargs):
        """
        Execute after a step was successfully completed.
        """

    @abstractmethod
    def error_step(self, *args, **kwargs):
        """
        Execute when error occurred while executing a step.
        """

    @abstractmethod
    def after_completion(self, *args, **kwargs):
        """
        Execute after a step
        (no matter the step was successfully completed or ended with an error)
        """


class ScenarioStatusListener(BaseListener):
    """
    Listener for scenario execution status
    """

    def before_scenario(self, worker):
        self._logger.info("Start scenario execution. %s" % (worker.get_scenario_queue_status()))

    def after_scenario(self, worker):
        self._logger.info("Finish scenario execution. %s" % (worker.get_scenario_queue_status()))


class StepStatusListener(StepListener):
    """
    This listener is only for logging.
    By default, Cliboa implements StepStatusListener in all steps.
    """

    def before_step(self, *args, **kwargs):
        self._logger.info("Start step execution. %s" % args[0].__class__.__name__)

    def after_step(self, *args, **kwargs):
        self._logger.info("Finish step execution. %s" % args[0].__class__.__name__)

    def after_completion(self, *args, **kwargs):
        self._logger.info("Complete step execution. %s" % args[0].__class__.__name__)
