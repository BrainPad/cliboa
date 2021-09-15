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
import json
import os
import shutil
import sys

import pytest
import yaml

from cliboa.client import CommandArgumentParser
from cliboa.conf import env
from cliboa.core.file_parser import JsonScenarioParser, YamlScenarioParser
from cliboa.test import BaseCliboaTest
from cliboa.util.exception import ScenarioFileInvalid


class TestYamlScenarioParser(BaseCliboaTest):
    def setUp(self):
        cmd_parser = CommandArgumentParser()
        sys.argv.clear()
        sys.argv.append("spam")
        sys.argv.append("spam")
        self._cmd_args = cmd_parser.parse()
        self._pj_dir = os.path.join(env.BASE_DIR, "project", "spam")
        self._cmn_dir = env.COMMON_DIR
        self._pj_scenario_file = os.path.join(
            env.BASE_DIR, "project", "spam", "scenario.yml"
        )
        self._cmn_scenario_dir = env.COMMON_SCENARIO_DIR
        self._cmn_scenario_file = os.path.join(env.COMMON_DIR, "scenario.yml")

        os.makedirs(self._pj_dir, exist_ok=True)
        os.makedirs(self._cmn_scenario_dir, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self._pj_dir, ignore_errors=True)

    def test_parse_with_pj_and_cmn_yaml_ok(self):
        """
        Valid project scenario.yml and common scenario.yml
        """
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

        cmn_yaml_dict = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._cmn_scenario_file, "w") as f:
            f.write(yaml.dump(cmn_yaml_dict, default_flow_style=False))

        exists_step = True
        try:
            parser = YamlScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            yaml_scenario = parser.parse()
            exists_step = any("step" in y for y in yaml_scenario)
        except Exception:
            exists_step = False
        assert exists_step is True

    def test_parse_with_pj_and_cmn_yaml_with_no_pj_args_ok(self):
        """
        Valid project scenario.yml and common scenario.yml.
        There is no arguments in project scenario.yml
        """
        pj_yaml_dict = {
            "scenario": [{"class": "SftpDownload", "step": "sftp_download"}]
        }
        with open(self._pj_scenario_file, "w") as f:
            f.write(yaml.dump(pj_yaml_dict, default_flow_style=False))

        cmn_yaml_dict = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._cmn_scenario_file, "w") as f:
            f.write(yaml.dump(cmn_yaml_dict, default_flow_style=False))

        exists_step = True
        try:
            parser = YamlScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            yaml_scenario = parser.parse()
            exists_step = any("step" in y for y in yaml_scenario)
        except Exception:
            exists_step = False
        assert exists_step is True

    def test_parse_with_pj_and_cmn_yaml_with_no_args_ok(self):
        """
        Valid project scenario.yml and common scenario.yml.
        In common scenario.yml, there is no arguments.
        """
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

        cmn_yaml_dict = {
            "scenario": [{"class": "SftpDownload", "step": "sftp_download"}]
        }
        with open(self._cmn_scenario_file, "w") as f:
            f.write(yaml.dump(cmn_yaml_dict, default_flow_style=False))

        exists_step = True
        try:
            parser = YamlScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            yaml_scenario = parser.parse()
            exists_step = any("step" in y for y in yaml_scenario)
        except Exception:
            exists_step = False
        assert exists_step is True

    def test_parse_with_pj_and_cmn_yaml_with_diff_cls_ok(self):
        """
        Valid project scenario.yml and common scenario.yml.
        In common scenario.yml, There are not same classes.
        """
        pj_yaml_dict = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SampleStep",
                    "step": "sample step",
                }
            ]
        }
        with open(self._pj_scenario_file, "w") as f:
            f.write(yaml.dump(pj_yaml_dict, default_flow_style=False))

        cmn_yaml_dict = {
            "scenario": [{"class": "SftpDownload", "step": "sftp_download"}]
        }
        with open(self._cmn_scenario_file, "w") as f:
            f.write(yaml.dump(cmn_yaml_dict, default_flow_style=False))

        exists_step = True
        try:
            parser = YamlScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            yaml_scenario = parser.parse()
            exists_step = any("step" in y for y in yaml_scenario)
        except Exception:
            exists_step = False
        assert exists_step is True

    def test_parse_with_no_cmn_yaml_ok(self):
        """
        Valid project scenario.yml, there is not common scenario.yml
        """
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

        exists_step = True
        try:
            parser = YamlScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            yaml_scenario = parser.parse()
            exists_step = any("step" in y for y in yaml_scenario)
        except Exception:
            exists_step = False
        assert exists_step is True

    def test_parse_no_scenario_key_pj_yaml_ng(self):
        """
        Invalid project scenario.yml
        """
        pj_yaml_dict = {
            "test": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._pj_scenario_file, "w") as f:
            f.write(yaml.dump(pj_yaml_dict, default_flow_style=False))

        cmn_yaml_dict = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._cmn_scenario_file, "w") as f:
            f.write(yaml.dump(cmn_yaml_dict, default_flow_style=False))

        with pytest.raises(ScenarioFileInvalid) as excinfo:
            parser = YamlScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            yaml_scenario = parser.parse()
            any("step" in y for y in yaml_scenario)
        assert "invalid" in str(excinfo.value)

    def test_parse_with_pj_and_cmn_yaml_no_class_ng(self):
        """
        project scenario.yml and common scenario.yml.
        There is no class: in project scenario.yml
        """
        pj_yaml_dict = {
            "scenario": [{"arguments": {"retry_count": 10}, "step": "sftp_download"}]
        }
        with open(self._pj_scenario_file, "w") as f:
            f.write(yaml.dump(pj_yaml_dict, default_flow_style=False))

        cmn_yaml_dict = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._cmn_scenario_file, "w") as f:
            f.write(yaml.dump(cmn_yaml_dict, default_flow_style=False))

        with pytest.raises(ScenarioFileInvalid) as excinfo:
            parser = YamlScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            yaml_scenario = parser.parse()
            any("step" in y for y in yaml_scenario)

        assert "invalid" in str(excinfo.value)

    def test_parse_with_pj_and_cmn_yaml_no_class_val_ng(self):
        """
        project scenario.yml and common scenario.yml.
        There is no class value: in common scenario.yml
        """
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

        cmn_yaml_dict = {
            "scenario": [
                {"arguments": {"retry_count": 10}, "class": "", "step": "sftp_download"}
            ]
        }
        with open(self._cmn_scenario_file, "w") as f:
            f.write(yaml.dump(cmn_yaml_dict, default_flow_style=False))

        with pytest.raises(ScenarioFileInvalid) as excinfo:
            parser = YamlScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            yaml_scenario = parser.parse()
            any("step" in y for y in yaml_scenario)

        assert "invalid" in str(excinfo.value)

    def test_parse_with_pj_and_cmn_yaml_parallel(self):
        """
        Test for parallel operation
        """
        pj_yaml_dict = {
            "scenario": [
                {
                    "parallel": [
                        {
                            "arguments": {"retry_count": 10},
                            "class": "SftpDownload",
                            "step": "sftp_download",
                        },
                        {
                            "arguments": {"retry_count": 10},
                            "class": "SftpDownload",
                            "step": "sftp_download",
                        },
                    ]
                }
            ]
        }
        with open(self._pj_scenario_file, "w") as f:
            f.write(yaml.dump(pj_yaml_dict, default_flow_style=False))

        cmn_yaml_dict = {
            "scenario": [
                {
                    "arguments": {"host": "dummy_host"},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._cmn_scenario_file, "w") as f:
            f.write(yaml.dump(cmn_yaml_dict, default_flow_style=False))

        parser = YamlScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
        yaml_scenario_list = parser.parse()

        for scenario in yaml_scenario_list:
            for dict in scenario.get("parallel"):
                assert "dummy_host" == dict.get("arguments")["host"]

    def test_parse_with_pj_and_cmn_yaml_parallel_with_config(self):
        """
        Test for parallel_with_config operation
        """
        pj_yaml_dict = {
            "scenario": [
                {
                    "parallel_with_config": {
                        "config": {
                            "multi_process_count": 5
                        },
                        "steps": [
                            {
                                "arguments": {"retry_count": 10},
                                "class": "SftpDownload",
                                "step": "sftp_download",
                            },
                            {
                                "arguments": {"retry_count": 10},
                                "class": "SftpDownload",
                                "step": "sftp_download",
                            },
                        ],
                    }
                }
            ]
        }
        with open(self._pj_scenario_file, "w") as f:
            f.write(yaml.dump(pj_yaml_dict, default_flow_style=False))

        cmn_yaml_dict = {
            "scenario": [
                {
                    "arguments": {"host": "dummy_host"},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._cmn_scenario_file, "w") as f:
            f.write(yaml.dump(cmn_yaml_dict, default_flow_style=False))

        parser = YamlScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
        yaml_scenario_list = parser.parse()

        for scenario in yaml_scenario_list:
            for dict in scenario.get("parallel_with_config").get("steps"):
                assert "dummy_host" == dict.get("arguments")["host"]


class TestJsonScenarioParser(BaseCliboaTest):
    def setUp(self):
        cmd_parser = CommandArgumentParser()
        sys.argv.clear()
        sys.argv.append("project_name")
        sys.argv.append("spam")
        sys.argv.append("--format")
        sys.argv.append("json")
        self._cmd_args = cmd_parser.parse()
        self._pj_dir = os.path.join(env.BASE_DIR, "project", "spam")
        self._cmn_dir = env.COMMON_DIR
        self._pj_scenario_file = os.path.join(
            env.BASE_DIR, "project", "spam", "scenario.json"
        )
        self._cmn_scenario_dir = env.COMMON_SCENARIO_DIR
        self._cmn_scenario_file = os.path.join(env.COMMON_DIR, "scenario.json")

        os.makedirs(self._pj_dir, exist_ok=True)
        os.makedirs(self._cmn_scenario_dir, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self._pj_dir, ignore_errors=True)

    def test_parse_with_pj_and_cmn_json_ok(self):
        """
        Valid project scenario.json and common scenario.json
        """
        pj_json_dict = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._pj_scenario_file, "w") as f:
            json.dump(pj_json_dict, f, indent=4)

        cmn_json_dict = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._cmn_scenario_file, "w") as f:
            json.dump(cmn_json_dict, f, indent=4)

        exists_step = True
        try:
            parser = JsonScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            json_scenario = parser.parse()
            exists_step = any("step" in j for j in json_scenario)
        except Exception:
            exists_step = False
        assert exists_step is True

    def test_parse_with_pj_and_cmn_json_with_no_pj_args_ok(self):
        """
        Valid project scenario.json and common scenario.json.
        There is no arguments in project scenario.json
        """
        pj_json_dict = {
            "scenario": [{"class": "SftpDownload", "step": "sftp_download"}]
        }
        with open(self._pj_scenario_file, "w") as f:
            json.dump(pj_json_dict, f, indent=4)

        cmn_json_dict = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._cmn_scenario_file, "w") as f:
            json.dump(cmn_json_dict, f, indent=4)

        exists_step = True
        try:
            parser = JsonScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            json_scenario = parser.parse()
            exists_step = any("step" in j for j in json_scenario)
        except Exception:
            exists_step = False
        assert exists_step is True

    def test_parse_with_pj_and_cmn_json_with_no_args_ok(self):
        """
        Valid project scenario.json and common scenario.json.
        In common scenario.json, there is no arguments.
        """
        pj_json_dict = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._pj_scenario_file, "w") as f:
            json.dump(pj_json_dict, f, indent=4)

        cmn_json_dict = {
            "scenario": [{"class": "SftpDownload", "step": "sftp_download"}]
        }
        with open(self._cmn_scenario_file, "w") as f:
            json.dump(cmn_json_dict, f, indent=4)

        exists_step = True
        try:
            parser = JsonScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            json_scenario = parser.parse()
            exists_step = any("step" in j for j in json_scenario)
        except Exception:
            exists_step = False
        assert exists_step is True

    def test_parse_with_pj_and_cmn_json_with_diff_cls_ok(self):
        """
        Valid project scenario.json and common scenario.json.
        In common scenario.json, There are not same classes.
        """
        pj_json_dict = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SampleStep",
                    "step": "sample step",
                }
            ]
        }
        with open(self._pj_scenario_file, "w") as f:
            json.dump(pj_json_dict, f, indent=4)

        cmn_json_dict = {
            "scenario": [{"class": "SftpDownload", "step": "sftp_download"}]
        }
        with open(self._cmn_scenario_file, "w") as f:
            json.dump(cmn_json_dict, f, indent=4)

        exists_step = True
        try:
            parser = JsonScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            json_scenario = parser.parse()
            exists_step = any("step" in j for j in json_scenario)
        except Exception:
            exists_step = False
        assert exists_step is True

    def test_parse_with_no_cmn_json_ok(self):
        """
        Valid project scenario.json, there is not common scenario.json
        """
        pj_json_dict = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._pj_scenario_file, "w") as f:
            json.dump(pj_json_dict, f, indent=4)

        exists_step = True
        try:
            parser = JsonScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            json_scenario = parser.parse()
            exists_step = any("step" in j for j in json_scenario)
        except Exception:
            exists_step = False
        assert exists_step is True

    def test_parse_no_scenario_key_pj_json_ng(self):
        """
        Invalid project scenario.json
        """
        pj_json_dict = {
            "test": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._pj_scenario_file, "w") as f:
            json.dump(pj_json_dict, f, indent=4)

        cmn_json_dict = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._cmn_scenario_file, "w") as f:
            json.dump(cmn_json_dict, f, indent=4)

        with pytest.raises(ScenarioFileInvalid) as excinfo:
            parser = JsonScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            json_scenario = parser.parse()
            any("step" in j for j in json_scenario)
        assert "invalid" in str(excinfo.value)

    def test_parse_with_pj_and_cmn_json_no_class_ng(self):
        """
        project scenario.json and common scenario.json.
        There is no class: in project scenario.json
        """
        pj_json_dict = {
            "scenario": [{"arguments": {"retry_count": 10}, "step": "sftp_download"}]
        }
        with open(self._pj_scenario_file, "w") as f:
            json.dump(pj_json_dict, f, indent=4)

        cmn_json_dict = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._cmn_scenario_file, "w") as f:
            json.dump(cmn_json_dict, f, indent=4)

        with pytest.raises(ScenarioFileInvalid) as excinfo:
            parser = JsonScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            json_scenario = parser.parse()
            any("step" in j for j in json_scenario)

        assert "invalid" in str(excinfo.value)

    def test_parse_with_pj_and_cmn_json_no_class_val_ng(self):
        """
        project scenario.json and common scenario.json.
        There is no class value: in common scenario.json
        """
        pj_json_dict = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._pj_scenario_file, "w") as f:
            json.dump(pj_json_dict, f, indent=4)

        cmn_json_dict = {
            "scenario": [
                {"arguments": {"retry_count": 10}, "class": "", "step": "sftp_download"}
            ]
        }
        with open(self._cmn_scenario_file, "w") as f:
            json.dump(cmn_json_dict, f, indent=4)

        with pytest.raises(ScenarioFileInvalid) as excinfo:
            parser = JsonScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            json_scenario = parser.parse()
            any("step" in j for j in json_scenario)

        assert "invalid" in str(excinfo.value)

    def test_parse_with_pj_and_cmn_json_parallel(self):
        """
        Test for parallel operation
        """
        pj_json_dict = {
            "scenario": [
                {
                    "parallel": [
                        {
                            "arguments": {"retry_count": 10},
                            "class": "SftpDownload",
                            "step": "sftp_download",
                        },
                        {
                            "arguments": {"retry_count": 10},
                            "class": "SftpDownload",
                            "step": "sftp_download",
                        },
                    ]
                }
            ]
        }
        with open(self._pj_scenario_file, "w") as f:
            json.dump(pj_json_dict, f, indent=4)

        cmn_json_dict = {
            "scenario": [
                {
                    "arguments": {"host": "dummy_host"},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._cmn_scenario_file, "w") as f:
            json.dump(cmn_json_dict, f, indent=4)

        parser = JsonScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
        json_scenario_list = parser.parse()

        for scenario in json_scenario_list:
            for dict in scenario.get("parallel"):
                assert "dummy_host" == dict.get("arguments")["host"]
