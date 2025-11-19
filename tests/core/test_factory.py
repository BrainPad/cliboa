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

import pytest

from cliboa.core.factory import _CliboaFactory
from cliboa.scenario import ExecuteShellScript
from cliboa.scenario.sample_step import SampleStep
from cliboa.util.exception import InvalidScenarioClass


class AttrDict(dict):
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(f"'AttrDict' object has no attribute '{key}'")


class TestCliboaFactory:
    def test_create_ok(self):
        instance = _CliboaFactory().create("ExecuteShellScript")
        assert type(instance) is ExecuteShellScript

    def test_create_ng(self):
        with pytest.raises(InvalidScenarioClass):
            _CliboaFactory().create("NotFoundClass")

    def test_create_custom_ok(self):
        mock_env = AttrDict(
            {
                "COMMON_CUSTOM_CLASSES": ["sample_step.SampleStep"],
                "PROJECT_CUSTOM_CLASSES": [],
                "COMMON_CUSTOM_ROOT_PATHS": ["cliboa.scenario"],
            }
        )
        instance = _CliboaFactory(di_env=mock_env).create("SampleStep")
        assert type(instance) is SampleStep

    def test_create_custom_multi_root_ok(self):
        mock_env = AttrDict(
            {
                "COMMON_CUSTOM_CLASSES": ["sample_step.SampleStep"],
                "PROJECT_CUSTOM_CLASSES": [],
                "COMMON_CUSTOM_ROOT_PATHS": ["hoge.fuga", "sample", "cliboa.scenario"],
            }
        )
        instance = _CliboaFactory(di_env=mock_env).create("SampleStep")
        assert type(instance) is SampleStep

    def test_create_custom_empty_root_paths_ok(self):
        mock_env = AttrDict(
            {
                "COMMON_CUSTOM_CLASSES": ["cliboa.scenario.sample_step.SampleStep"],
                "PROJECT_CUSTOM_CLASSES": [],
                "COMMON_CUSTOM_ROOT_PATHS": [""],
            }
        )
        instance = _CliboaFactory(di_env=mock_env).create("SampleStep")
        assert type(instance) is SampleStep

    def test_create_custom_ng(self):
        mock_env = AttrDict(
            {
                "COMMON_CUSTOM_CLASSES": [],
                "PROJECT_CUSTOM_CLASSES": [],
                "COMMON_CUSTOM_ROOT_PATHS": ["cliboa.scenario"],
            }
        )
        with pytest.raises(InvalidScenarioClass):
            _CliboaFactory(di_env=mock_env).create("SampleStep")

    def test_create_prj_ng(self):
        mock_env = AttrDict(
            {
                "COMMON_CUSTOM_CLASSES": [],
                "PROJECT_CUSTOM_CLASSES": ["sample_step.SampleStep"],
                "COMMON_CUSTOM_ROOT_PATHS": [],
                "PROJECT_CUSTOM_ROOT_PATHS": ["project"],
                "PROJECT_SCENARIO_DIR_NAME": None,
            }
        )
        with pytest.raises(InvalidScenarioClass):
            _CliboaFactory("scenario", di_env=mock_env).create("SampleStep")

    def test_create_prj_dir_none_ok(self):
        mock_env = AttrDict(
            {
                "COMMON_CUSTOM_CLASSES": [],
                "PROJECT_CUSTOM_CLASSES": ["sample_step.SampleStep"],
                "COMMON_CUSTOM_ROOT_PATHS": [],
                "PROJECT_CUSTOM_ROOT_PATHS": ["cliboa"],
                "PROJECT_SCENARIO_DIR_NAME": None,
            }
        )
        instance = _CliboaFactory("scenario", di_env=mock_env).create("SampleStep")
        assert type(instance) is SampleStep

    def test_create_prj_dir_scenario_ok(self):
        mock_env = AttrDict(
            {
                "COMMON_CUSTOM_CLASSES": [],
                "PROJECT_CUSTOM_CLASSES": ["SampleStep"],
                "COMMON_CUSTOM_ROOT_PATHS": [],
                "PROJECT_CUSTOM_ROOT_PATHS": ["cliboa"],
                "PROJECT_SCENARIO_DIR_NAME": "sample_step",
            }
        )
        instance = _CliboaFactory("scenario", di_env=mock_env).create("SampleStep")
        assert type(instance) is SampleStep
