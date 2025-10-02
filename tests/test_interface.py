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

from cliboa.conf import env
from cliboa.core.runner import ScenarioRunner
from cliboa.interface import _add_system_path as if_add_system_path
from cliboa.interface import _parse as if_parse
from tests import BaseCliboaTest


class TestCommandArgumentParser(BaseCliboaTest):
    def setup_method(self, method):
        """
        setup environment for test
        """
        sys.argv.clear()
        sys.argv.append("")
        sys.argv.append("spam")

    def test_yaml_parse(self):
        cmd_args = if_parse()
        assert cmd_args.project_name == "spam"

    def test_json_parse(self):
        sys.argv.clear()
        sys.argv.append("project_name")
        sys.argv.append("spam")
        sys.argv.append("--format")
        sys.argv.append("json")
        cmd_args = if_parse()
        assert cmd_args.project_name == "spam"
        assert cmd_args.format == "json"

    def test_add_system_path(self):
        if_add_system_path("spam")
        includes_spam = any("project/spam" in p for p in sys.path)
        self.assertTrue(includes_spam)


@pytest.mark.skip(reason="ScenarioRunner is scheduled for a redesign for v3.")
class TestScenarioRunner(BaseCliboaTest):
    def setup_method(self, method):
        self._pj_dir = os.path.join(env.BASE_DIR, "project", "spam")
        self._scenario_yaml_file = os.path.join(self._pj_dir, "scenario.yml")
        self._scenario_json_file = os.path.join(self._pj_dir, "scenario.json")

    @pytest.fixture(autouse=True)
    def setup_resource(self):
        os.makedirs(self._pj_dir)
        yield "test in progress"
        shutil.rmtree(self._pj_dir)

    def test_create_scenario_queue_ok(self):
        test_data = {
            "scenario": [
                {
                    "arguments": {"src_dir": self._pj_dir, "src_pattern": r"(.*)\.csv"},
                    "class": "FileRename",
                    "step": "",
                }
            ]
        }

        with open(self._scenario_yaml_file, "w") as f:
            f.write(yaml.dump(test_data, default_flow_style=False))

        runner = ScenarioRunner("spam")
        runner._create_scenario_queue()

        with open(self._scenario_json_file, "w") as f:
            json.dump(test_data, f, indent=4)

        runner = ScenarioRunner("spam", "json")
        runner._create_scenario_queue()

    def test_execute_scenario_ok(self):
        test_data = {
            "scenario": [
                {
                    "arguments": {"src_dir": self._pj_dir, "src_pattern": r"(.*)\.csv"},
                    "class": "FileRename",
                    "step": "",
                }
            ]
        }
        with open(self._scenario_yaml_file, "w") as f:
            f.write(yaml.dump(test_data, default_flow_style=False))

        runner = ScenarioRunner("spam")
        runner._execute_scenario()

        with open(self._scenario_json_file, "w") as f:
            json.dump(test_data, f, indent=4)

        runner = ScenarioRunner("spam", "json")
        runner._execute_scenario()
