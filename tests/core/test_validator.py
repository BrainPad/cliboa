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
import os
import shutil

import pytest

from cliboa.conf import env
from cliboa.core.validator import EssentialKeys
from cliboa.util.exception import ScenarioFileInvalid


class TestValidators(object):
    def setup_method(self, method):
        self._pj_dir = os.path.join(env.BASE_DIR, "project", "spam")
        self._scenario_file = os.path.join(env.BASE_DIR, "project", "spam", "scenario.yml")
        os.makedirs(self._pj_dir, exist_ok=True)

    def teardown_method(self, method):
        if os.path.exists(self._pj_dir):
            shutil.rmtree(self._pj_dir)

    def test_essential_keys_ok_1(self):
        """
        Block requires both "step" and "class"
        """
        test_yaml = [{"step": "test step", "class": "SampleClass"}]
        valid_instance = EssentialKeys(test_yaml)
        valid_instance()

    def test_essential_keys_ok_2(self):
        """
        If block starts with "parallel"
        all steps under the "parallel" requires both "step" and "class"
        """
        test_yaml = [
            {
                "parallel": [
                    {"step": "test step 1", "class": "SampleClass"},
                    {"step": "test step 2", "class": "SampleClass"},
                ]
            }
        ]
        valid_instance = EssentialKeys(test_yaml)
        valid_instance()

    def test_essential_keys_ok_3(self):
        """
        If block starts with "multi_process_count"
        does not require "step" or "class"
        """
        test_yaml = [{"multi_process_count": 1}]
        valid_instance = EssentialKeys(test_yaml)
        valid_instance()

    def test_essential_keys_ok_4(self):
        """
        If block starts with "parallel_with_config"
        all steps under the "parallel_with_config" requires both "step" and "class"
        """
        test_yaml = [
            {
                "parallel_with_config": {
                    "config": {"multi_process_count": 2},
                    "steps": [
                        {"step": "test step 1", "class": "SampleClass"},
                        {"step": "test step 2", "class": "SampleClass"},
                    ],
                }
            }
        ]
        valid_instance = EssentialKeys(test_yaml)
        valid_instance()

    def test_essential_keys_ng1(self):
        """
        Block requires both "step" and "class"
        """
        test_yaml = [{"class": "SampleClass"}]
        with pytest.raises(ScenarioFileInvalid) as excinfo:
            valid_instance = EssentialKeys(test_yaml)
            valid_instance()
        assert "scenario file is invalid. 'step:' does not exist." in str(excinfo.value)

    def test_essential_keys_ng_2(self):
        """
        If block starts with "parallel"
        all steps under the "parallel" requires both "step" and "class"
        """
        test_yaml = [
            {
                "parallel": [
                    {"step": "test step 1", "class": "SampleClass"},
                    {"class": "SampleClass"},
                ]
            }
        ]
        with pytest.raises(ScenarioFileInvalid) as excinfo:
            valid_instance = EssentialKeys(test_yaml)
            valid_instance()
        assert "scenario file is invalid. 'step:' does not exist." in str(excinfo.value)

    def test_essential_keys_ng_3(self):
        """
        If block starts with "parallel_with_config"
        all steps under the "parallel_with_config" requires both "step" and "class"
        """
        test_yaml = [
            {
                "parallel_with_config": {
                    "steps": [
                        {"step": "test step 1", "class": "SampleClass"},
                        {"step": "test step 2", "class": "SampleClass"},
                    ],
                }
            }
        ]
        with pytest.raises(ScenarioFileInvalid) as excinfo:
            valid_instance = EssentialKeys(test_yaml)
            valid_instance()
        assert (
            "scenario file is invalid. 'config:' key does not exist, or 'config:' value does not exist."  # noqa
            in str(excinfo.value)
        )  # noqa

    def test_essential_keys_ng_4(self):
        """
        If block starts with "parallel_with_config"
        all steps under the "parallel_with_config" requires both "step" and "class"
        """
        test_yaml = [{"parallel_with_config": {"config": {"multi_process_count": 2}}}]
        with pytest.raises(ScenarioFileInvalid) as excinfo:
            valid_instance = EssentialKeys(test_yaml)
            valid_instance()
        assert (
            "scenario file is invalid. 'steps:' key does not exist, or 'steps:' value does not exist."  # noqa
            in str(excinfo.value)
        )  # noqa

    def test_essential_keys_ng_5(self):
        """
        If block starts with "parallel_with_config"
        all steps under the "parallel_with_config" requires both "step" and "class"
        """
        test_yaml = [
            {
                "parallel_with_config": {
                    "config": {"multi_process_count": 2},
                    "steps": [
                        {"step": "test step 1", "class": "SampleClass"},
                        {"class": "SampleClass"},
                    ],
                }
            }
        ]
        with pytest.raises(ScenarioFileInvalid) as excinfo:
            valid_instance = EssentialKeys(test_yaml)
            valid_instance()
        assert "scenario file is invalid. 'step:' does not exist." in str(excinfo.value)
