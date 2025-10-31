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
from unittest.mock import Mock

from cliboa.core.executor import _ScenarioExecutor
from cliboa.listener.scenario import ScenarioStatusListener
from cliboa.listener.step import StepStatusListener
from cliboa.scenario.sample_step import SampleCustomStep


class TestScenarioStatusListener:
    """
    Tests for ScenarioStatusListener.
    """

    def setup_method(self, method):
        """
        Set up ScenarioStatusListener with mock logger.
        """
        self._mock_logger = Mock()
        self._listener = ScenarioStatusListener(di_logger=self._mock_logger)
        self._executor = _ScenarioExecutor([])

    def test_completion(self):
        """
        Test completion log using mock.
        Ensures completion log is the last log message.
        """
        self._listener.completion(self._executor)

        assert self._mock_logger.info.call_count > 0
        args, kwargs = self._mock_logger.info.call_args
        log_message = args[0]
        assert "Complete" in log_message

    def test_before(self):
        """
        Test before log using mock.
        Ensures before log is the last log message.
        """
        self._listener.before(self._executor)

        assert self._mock_logger.info.call_count > 0
        args, kwargs = self._mock_logger.info.call_args
        log_message = args[0]
        assert "Start" in log_message


class TestStepStatusListener:
    """
    Tests for StepStatusListener.
    """

    def setup_method(self, method):
        """
        Set up StepStatusListener with mock logger.
        """
        self._mock_logger = Mock()
        self._listener = StepStatusListener(di_logger=self._mock_logger)
        self._step = SampleCustomStep()

    def test_after(self):
        """
        Test after log using mock.
        Ensures after log is the last log message.
        """
        self._listener.after(self._step)

        assert self._mock_logger.info.call_count > 0
        args, kwargs = self._mock_logger.info.call_args

        log_message = args[0]
        assert "Finish" in log_message
        assert self._step.__class__.__name__ in log_message

    def test_before(self):
        """
        Test before log using mock.
        Ensures before log is the last log message.
        """
        self._listener.before(self._step)

        assert self._mock_logger.info.call_count > 0
        args, kwargs = self._mock_logger.info.call_args

        log_message = args[0]
        assert "Start" in log_message
        assert self._step.__class__.__name__ in log_message
