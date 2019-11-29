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
import os
import pytest
import shutil
import sys
import yaml
from pprint import pprint

from cliboa.client import CommandArgumentParser
from cliboa.conf import env
from cliboa.core.manager import YamlScenarioManager
from cliboa.core.scenario_queue import ScenarioQueue
from cliboa.util.exception import *


class TestYamlScenarioManager(object):
    def setup_method(self, method):
        cmd_parser = CommandArgumentParser()
        sys.argv.clear()
        sys.argv.append("spam")
        sys.argv.append("spam")
        self._cmd_args = cmd_parser.parse()
        self._pj_dir = os.path.join(env.PROJECT_DIR, "spam")
        self._pj_scenario_file = os.path.join(env.PROJECT_DIR, "spam", "scenario.yml")

    def test_create_scenario_queue_ok(self):
        """
        Valid scenario.yml
        """
        os.makedirs(self._pj_dir)
        pj_yaml_dict = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._pj_scenario_file, "w") as f:
            f.write(yaml.dump(pj_yaml_dict, default_flow_style=False))

        is_completed_queue_creation = True
        try:
            manager = YamlScenarioManager(self._cmd_args)
            manager.create_scenario_queue()
            ScenarioQueue.step_queue.pop()
        except Exception:
            is_completed_queue_creation = False
        else:
            shutil.rmtree(self._pj_dir)
        assert is_completed_queue_creation is True

    def test_create_scenario_queue_ok_with_no_args(self):
        """
        Valid scenario.yml with no arguments
        """
        os.makedirs(self._pj_dir)
        pj_yaml_dict = {
            "scenario": [{"class": "SftpDownload", "step": "sftp_download"}]
        }
        with open(self._pj_scenario_file, "w") as f:
            f.write(yaml.dump(pj_yaml_dict, default_flow_style=False))

        is_completed_queue_creation = True
        try:
            manager = YamlScenarioManager(self._cmd_args)
            manager.create_scenario_queue()
            ScenarioQueue.step_queue.pop()
        except Exception:
            is_completed_queue_creation = False
        else:
            shutil.rmtree(self._pj_dir)
        assert is_completed_queue_creation is True

    def test_create_scenario_queue_ok_with_di_and_diargs(self):
        """
        Valid scenario.yml with dependency injection
        """
        os.makedirs(self._pj_dir)
        pj_yaml = {
            "scenario": [
                {
                    "step": "spam",
                    "class": "HttpDownload",
                    "arguments": {
                        "src_url": "https://spam/",
                        "auth": {
                            "class": "FormAuth",
                            "form_id": "spam",
                            "form_password": "spam",
                            "form_url": "http://spam/",
                        },
                    },
                }
            ]
        }

        with open(self._pj_scenario_file, "w") as f:
            f.write(yaml.dump(pj_yaml, default_flow_style=False))

        is_completed_queue_creation = True
        try:
            manager = YamlScenarioManager(self._cmd_args)
            manager.create_scenario_queue()
            ScenarioQueue.step_queue.pop()
        except Exception:
            is_completed_queue_creation = False
        else:
            shutil.rmtree(self._pj_dir)
        assert is_completed_queue_creation is True

    def test_create_scenario_queue_ng_with_di_and_invalid_diargs(self):
        """
        scenario.yml with dependency injection with invalid arguments
        """
        os.makedirs(self._pj_dir)
        pj_yaml = {
            "scenario": [
                {
                    "step": "spam",
                    "class": "HttpDownload",
                    "arguments": {
                        "auth": {"form_id": "spam"},
                        "src_url": "https://spam/",
                    },
                }
            ]
        }
        with open(self._pj_scenario_file, "w") as f:
            f.write(yaml.dump(pj_yaml, default_flow_style=False))

        with pytest.raises(ScenarioFileInvalid) as excinfo:
            manager = YamlScenarioManager(self._cmd_args)
            manager.create_scenario_queue()
        shutil.rmtree(self._pj_dir)
        assert "class: is not specified" in str(excinfo.value)

    def test_create_scenario_queue_ok_with_vars(self):
        """
        Valid scenario.yml with {{ vars }}
        """
        os.makedirs(self._pj_dir)
        pj_yaml_dict = {
            "scenario": [
                {
                    "arguments": {
                        "src_pattern": "foo_{{ today }}.csv",
                        "with_vars": {"today": "date '+%Y%m%d'"},
                    },
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._pj_scenario_file, "w") as f:
            f.write(yaml.dump(pj_yaml_dict, default_flow_style=False))

        is_completed_queue_creation = True
        try:
            manager = YamlScenarioManager(self._cmd_args)
            manager.create_scenario_queue()
            ScenarioQueue.step_queue.pop()
        except Exception:
            is_completed_queue_creation = False
        else:
            shutil.rmtree(self._pj_dir)
        assert is_completed_queue_creation is True

    def test_create_scenario_queue_ng(self):
        """
        Invalid scenario.yml
        """
        os.makedirs(self._pj_dir)
        pj_yaml_dict = {"scenario": ["arguments", "spam"]}
        with open(self._pj_scenario_file, "w") as f:
            f.write(yaml.dump(pj_yaml_dict, default_flow_style=False))

        with pytest.raises(AttributeError) as excinfo:
            manager = YamlScenarioManager(self._cmd_args)
            manager.create_scenario_queue()
        shutil.rmtree(self._pj_dir)
        assert "object has no attribute" in str(excinfo.value)

    def test_create_scenario_queue_with_no_list_ng(self):
        """
        Invalid scenario.yml
        """
        os.makedirs(self._pj_dir)
        pj_yaml_dict = {"scenario": {"arguments", "spam"}}
        with open(self._pj_scenario_file, "w") as f:
            f.write(yaml.dump(pj_yaml_dict, default_flow_style=False))

        with pytest.raises(ScenarioFileInvalid) as excinfo:
            manager = YamlScenarioManager(self._cmd_args)
            manager.create_scenario_queue()
        shutil.rmtree(self._pj_dir)
        assert "invalid" in str(excinfo.value)
