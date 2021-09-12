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
import sys
import pytest

from cliboa.client import CommandArgumentParser
from cliboa.conf import env
from cliboa.core.validator import (
    ProjectDirectoryExistence,
    ScenarioFileExistence,
    ScenarioJsonKey,
    ScenarioJsonType,
    ScenarioYamlKey,
    ScenarioYamlType,
    EssentialKeys,
)
from cliboa.util.exception import FileNotFound, ScenarioFileInvalid, DirStructureInvalid


class TestValidators(object):
    def setup_method(self, method):
        CommandArgumentParser()
        sys.argv.clear()
        sys.argv.append("spam")
        sys.argv.append("spam")
        self._pj_dir = os.path.join(env.BASE_DIR, "project", "spam")
        self._scenario_file = os.path.join(
            env.BASE_DIR, "project", "spam", "scenario.yml"
        )
        os.makedirs(self._pj_dir, exist_ok=True)

    def teardown_method(self, method):
        if os.path.exists(self._pj_dir):
            shutil.rmtree(self._pj_dir)

    def test_scenario_yaml_type_ok(self):
        """
        scenario.yml type is valid
        """
        test_data = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        valid_instance = ScenarioYamlType(test_data)
        ret = valid_instance()
        assert ret is None

    def test_scenario_yaml_type_ng(self):
        """
        scenario.yml type is invalid
        """
        test_data = [
            {
                "scenario": {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            }
        ]
        with pytest.raises(ScenarioFileInvalid) as excinfo:
            valid_instance = ScenarioYamlType(test_data)
            valid_instance()
        assert "scenario.yml is invalid. Check scenario.yml format." in str(
            excinfo.value
        )

    def test_scenario_yaml_key_ok(self):
        """
        scenario.yml essential key is valid
        """
        test_data = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        valid_instance = ScenarioYamlKey(test_data)
        ret = valid_instance()
        assert ret is None

    def test_scenario_yaml_key_ng_with_no_content(self):
        """
        scenario.yml essential key exists, but content does not exist.
        """
        test_data = {"scenario": ""}
        with pytest.raises(ScenarioFileInvalid) as excinfo:
            valid_instance = ScenarioYamlKey(test_data)
            valid_instance()
        assert (
            "scenario.yml is invalid. 'scenario:' key does not exist, or 'scenario:' key exists but content under 'scenario:' key does not exist."  # noqa
            in str(excinfo.value)
        )

    def test_scenario_yaml_key_ng_with_no_scenario_key(self):
        """
        scenario.yml essential key does not exist.
        """
        test_data = {"spam": ""}
        with pytest.raises(ScenarioFileInvalid) as excinfo:
            valid_instance = ScenarioYamlKey(test_data)
            valid_instance()
        assert (
            "scenario.yml is invalid. 'scenario:' key does not exist, or 'scenario:' key exists but content under 'scenario:' key does not exist."  # noqa
            in str(excinfo.value)
        )

    def test_scenario_json_type_ok(self):
        """
        scenario.json type is valid
        """
        test_data = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        valid_instance = ScenarioJsonType(test_data)
        ret = valid_instance()
        assert ret is None

    def test_scenario_json_type_ng(self):
        """
        scenario.json type is invalid
        """
        test_data = [
            {
                "scenario": {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            }
        ]
        with pytest.raises(ScenarioFileInvalid) as excinfo:
            valid_instance = ScenarioJsonType(test_data)
            valid_instance()
        assert "scenario.json is invalid. Check scenario.json format." in str(
            excinfo.value
        )

    def test_scenario_json_key_ok(self):
        """
        scenario.json essential key is valid
        """
        test_data = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        valid_instance = ScenarioJsonKey(test_data)
        ret = valid_instance()
        assert ret is None

    def test_scenario_json_key_ng_with_no_content(self):
        """
        scenario.json essential key exists, but content does not exist.
        """
        test_data = {"scenario": ""}
        with pytest.raises(ScenarioFileInvalid) as excinfo:
            valid_instance = ScenarioJsonKey(test_data)
            valid_instance()
        assert (
            "scenario.json is invalid. 'scenario:' key does not exist, or 'scenario:' key exists but content under 'scenario:' key does not exist."  # noqa
            in str(excinfo.value)
        )

    def test_scenario_json_key_ng_with_no_scenario_key(self):
        """
        scenario.json essential key does not exist.
        """
        test_data = {"spam": ""}
        with pytest.raises(ScenarioFileInvalid) as excinfo:
            valid_instance = ScenarioJsonKey(test_data)
            valid_instance()
        assert (
            "scenario.json is invalid. 'scenario:' key does not exist, or 'scenario:' key exists but content under 'scenario:' key does not exist."  # noqa
            in str(excinfo.value)
        )

    def test_project_directory_existence_ok(self):
        """
        Specified directory exists
        """
        valid_instance = ProjectDirectoryExistence()
        ret = valid_instance(self._pj_dir)
        assert ret is None

    def test_project_directory_existence_ng(self):
        """
        Specified directory does not exist
        """
        shutil.rmtree(self._pj_dir)
        with pytest.raises(DirStructureInvalid) as excinfo:
            valid_instance = ProjectDirectoryExistence()
            valid_instance(self._pj_dir)
        assert "Project directory %s does not exist" % self._pj_dir in str(
            excinfo.value
        )

    def test_scenario_file_existence_ok(self):
        """
        Specified file exists
        """
        scenario_file = os.path.join(self._pj_dir, "scenario.yml")
        open(scenario_file, "w").close
        valid_instance = ScenarioFileExistence()
        ret = valid_instance(scenario_file)
        assert ret is None

    def test_scenario_file_existence_ng(self):
        """
        Specified file does not exist
        """
        scenario_file = os.path.join(self._pj_dir, "scenario.yml")
        valid_instance = ScenarioFileExistence()
        with pytest.raises(FileNotFound) as excinfo:
            valid_instance(scenario_file)
        assert "scenario.yml %s does not exist" % scenario_file in str(excinfo.value)

    def test_essential_keys_ok_1(self):
        """
        Block requires both "step" and "class"
        """
        test_yaml = [{"step": "test step", "class": "SampleClass",}]
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
                    {"step": "test step 1", "class": "SampleClass",},
                    {"step": "test step 2", "class": "SampleClass",},
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

    def test_essential_keys_ng1(self):
        """
        Block requires both "step" and "class"
        """
        test_yaml = [{"class": "SampleClass"}]
        with pytest.raises(ScenarioFileInvalid) as excinfo:
            valid_instance = EssentialKeys(test_yaml)
            valid_instance()
        assert "scenario.yml is invalid. 'step:' does not exist." in str(excinfo.value)

    def test_essential_keys_ng_2(self):
        """
        If block starts with "parallel"
        all steps under the "parallel" requires both "step" and "class"
        """
        test_yaml = [
            {
                "parallel": [
                    {"step": "test step 1", "class": "SampleClass",},
                    {"class": "SampleClass",},
                ]
            }
        ]
        with pytest.raises(ScenarioFileInvalid) as excinfo:
            valid_instance = EssentialKeys(test_yaml)
            valid_instance()
        assert "scenario.yml is invalid. 'step:' does not exist." in str(excinfo.value)
