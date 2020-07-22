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
import shutil
import sys

import pytest
import yaml

from cliboa.client import CommandArgumentParser
from cliboa.conf import env
from cliboa.core.file_parser import YamlScenarioParser
from cliboa.util.exception import ScenarioFileInvalid


class TestYamlScenarioParser(object):
    def setup_method(self, method):
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

    def test_parse_with_pj_and_cmn_yaml_ok(self):
        """
        Valid project scenario.yml and common scenario.yml
        """
        os.makedirs(self._pj_dir)
        os.makedirs(self._cmn_scenario_dir)
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
        else:
            shutil.rmtree(self._pj_dir)
            shutil.rmtree(self._cmn_dir)
        assert exists_step is True

    def test_parse_with_pj_and_cmn_yaml_with_no_pj_args_ok(self):
        """
        Valid project scenario.yml and common scenario.yml.
        There is no arguments in project scenario.yml
        """
        os.makedirs(self._pj_dir)
        os.makedirs(self._cmn_scenario_dir)
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
        else:
            shutil.rmtree(self._pj_dir)
            shutil.rmtree(self._cmn_dir)
        assert exists_step is True

    def test_parse_with_pj_and_cmn_yaml_with_no_args_ok(self):
        """
        Valid project scenario.yml and common scenario.yml.
        In common scenario.yml, there is no arguments.
        """
        os.makedirs(self._pj_dir)
        os.makedirs(self._cmn_scenario_dir)
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
        else:
            shutil.rmtree(self._pj_dir)
            shutil.rmtree(self._cmn_dir)
        assert exists_step is True

    def test_parse_with_pj_and_cmn_yaml_with_diff_cls_ok(self):
        """
        Valid project scenario.yml and common scenario.yml.
        In common scenario.yml, There are not same classes.
        """
        os.makedirs(self._pj_dir)
        os.makedirs(self._cmn_scenario_dir)
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
        else:
            shutil.rmtree(self._pj_dir)
            shutil.rmtree(self._cmn_dir)
        assert exists_step is True

    def test_parse_with_no_cmn_yaml_ok(self):
        """
        Valid project scenario.yml, there is not common scenario.yml
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

        exists_step = True
        try:
            parser = YamlScenarioParser(self._pj_scenario_file, self._cmn_scenario_file)
            yaml_scenario = parser.parse()
            exists_step = any("step" in y for y in yaml_scenario)
        except Exception:
            exists_step = False
        else:
            shutil.rmtree(self._pj_dir)
        assert exists_step is True

    def test_parse_no_scenario_key_pj_yaml_ng(self):
        """
        Invalid project scenario.yml
        """
        os.makedirs(self._pj_dir)
        os.makedirs(self._cmn_scenario_dir)
        pj_yaml_dict = [
            {
                "arguments": {"retry_count": 10},
                "class": "SftpDownload",
                "step": "sftp_download",
            }
        ]
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
        shutil.rmtree(self._pj_dir)
        shutil.rmtree(self._cmn_dir)
        assert "invalid" in str(excinfo.value)

    def test_parse_with_pj_and_cmn_yaml_no_class_ng(self):
        """
        project scenario.yml and common scenario.yml.
        There is no class: in project scenario.yml
        """
        os.makedirs(self._pj_dir)
        os.makedirs(self._cmn_scenario_dir)
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

        shutil.rmtree(self._pj_dir)
        shutil.rmtree(self._cmn_dir)
        assert "invalid" in str(excinfo.value)

    def test_parse_with_pj_and_cmn_yaml_no_class_val_ng(self):
        """
        project scenario.yml and common scenario.yml.
        There is no class value: in common scenario.yml
        """
        os.makedirs(self._pj_dir)
        os.makedirs(self._cmn_scenario_dir)
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

        shutil.rmtree(self._pj_dir)
        shutil.rmtree(self._cmn_dir)
        assert "invalid" in str(excinfo.value)
