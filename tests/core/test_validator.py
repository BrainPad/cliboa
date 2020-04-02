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
from cliboa.core.validator import (ProjectDirectoryExistence,
                                   ScenarioFileExistence, ScenarioYamlKey,
                                   ScenarioYamlType)
from cliboa.util.exception import FileNotFound, ScenarioFileInvalid


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

    def test_scenario_yaml_type_ok(self):
        """
        scenario.yml type is valid
        """
        os.makedirs(self._pj_dir)
        test_data = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._scenario_file, "w") as f:
            f.write(yaml.dump(test_data, default_flow_style=False))
        pj_f = open(self._scenario_file, "r")
        pj_yaml_dict = yaml.safe_load(pj_f)
        is_dict = True
        try:
            valid_instance = ScenarioYamlType(pj_yaml_dict)
            valid_instance()
        except Exception:
            is_dict = False
        else:
            pj_f.close()
            shutil.rmtree(self._pj_dir)
        assert is_dict is True

    def test_scenario_yaml_type_ng(self):
        """
        scenario.yml type is invalid
        """
        os.makedirs(self._pj_dir)
        test_data = [
            {
                "scenario": {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            }
        ]
        with open(self._scenario_file, "w") as f:
            f.write(yaml.dump(test_data, default_flow_style=False))
        pj_f = open(self._scenario_file, "r")
        pj_yaml_dict = yaml.safe_load(pj_f)
        is_dict = True
        try:
            valid_instance = ScenarioYamlType(pj_yaml_dict)
            valid_instance()
        except Exception:
            is_dict = False
        finally:
            pj_f.close()
            shutil.rmtree(self._pj_dir)
        assert is_dict is False

    def test_scenario_yaml_key_ok(self):
        """
        scenario.yml essential key is valid
        """
        os.makedirs(self._pj_dir)
        test_data = {
            "scenario": [
                {
                    "arguments": {"retry_count": 10},
                    "class": "SftpDownload",
                    "step": "sftp_download",
                }
            ]
        }
        with open(self._scenario_file, "w") as f:
            f.write(yaml.dump(test_data, default_flow_style=False))
        pj_f = open(self._scenario_file, "r")
        pj_yaml_dict = yaml.safe_load(pj_f)
        is_valid_yml = True
        try:
            valid_instance = ScenarioYamlKey(pj_yaml_dict)
            valid_instance()
        except Exception:
            is_valid_yml = False
        else:
            pj_f.close()
            shutil.rmtree(self._pj_dir)
        assert is_valid_yml is True

    def test_scenario_yaml_key_ng_with_no_content(self):
        """
        scenario.yml essential key exists, but content does not exist.
        """
        os.makedirs(self._pj_dir)
        test_data = {"scenario": ""}
        with open(self._scenario_file, "w") as f:
            f.write(yaml.dump(test_data, default_flow_style=False))
        pj_f = open(self._scenario_file, "r")
        pj_yaml_dict = yaml.safe_load(pj_f)
        with pytest.raises(ScenarioFileInvalid) as excinfo:
            valid_instance = ScenarioYamlKey(pj_yaml_dict)
            valid_instance()
        shutil.rmtree(self._pj_dir)
        assert "invalid" in str(excinfo.value)

    def test_scenario_yaml_key_ng_with_no_scenario_key(self):
        """
        scenario.yml essential key does not exist.
        """
        os.makedirs(self._pj_dir)
        test_data = {"spam": ""}
        with open(self._scenario_file, "w") as f:
            f.write(yaml.dump(test_data, default_flow_style=False))
        pj_f = open(self._scenario_file, "r")
        pj_yaml_dict = yaml.safe_load(pj_f)
        with pytest.raises(ScenarioFileInvalid) as excinfo:
            valid_instance = ScenarioYamlKey(pj_yaml_dict)
            valid_instance()
        shutil.rmtree(self._pj_dir)
        assert "scenario" in str(excinfo.value)

    def test_project_directory_existence_ok(self):
        """
        Specified directory exists
        """
        os.makedirs(self._pj_dir)
        valid_instance = ProjectDirectoryExistence()
        exists_pj_dir = True
        try:
            valid_instance(self._pj_dir)
        except Exception:
            exists_pj_dir = False
        finally:
            shutil.rmtree(self._pj_dir)
        assert exists_pj_dir is True

    def test_project_directory_existence_ng(self):
        """
        Specified directory does not exist
        """
        valid_instance = ProjectDirectoryExistence()
        exists_pj_dir = True
        try:
            valid_instance(self._pj_dir)
        except Exception:
            exists_pj_dir = False
        assert exists_pj_dir is False

    def test_scenario_file_existence_ok(self):
        """
        Specified file exists
        """
        os.makedirs(self._pj_dir)
        scenario_file = os.path.join(self._pj_dir, "scenario.yml")
        open(scenario_file, "w").close
        valid_instance = ScenarioFileExistence()
        exists_scenario_file = True
        try:
            valid_instance(scenario_file)
        except Exception:
            exists_scenario_file = False
        finally:
            shutil.rmtree(self._pj_dir)
        assert exists_scenario_file is True

    def test_scenario_file_existence_ng(self):
        """
        Specified file does not exist
        """
        scenario_file = os.path.join(self._pj_dir, "scenario.yml")
        valid_instance = ScenarioFileExistence()
        with pytest.raises(FileNotFound) as excinfo:
            valid_instance(scenario_file)
        assert "not exist" in str(excinfo.value)
