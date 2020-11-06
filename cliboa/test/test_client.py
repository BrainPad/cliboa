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

from cliboa.client import CommandArgumentParser, ScenarioRunner
from cliboa.conf import env
from cliboa.test import BaseCliboaTest
from cliboa.util.exception import FileNotFound


class TestCommandArgumentParser(BaseCliboaTest):
    def setup_method(self, method):
        """
        setup environment for test
        """
        sys.argv.clear()
        sys.argv.append("")
        sys.argv.append("spam")

    def test_parse(self):
        cmd_parser = CommandArgumentParser()
        cmd_args = cmd_parser.parse()
        assert cmd_args.project_name == "spam"


class TestScenarioRunner(BaseCliboaTest):
    def setup_method(self, method):
        cmd_parser = CommandArgumentParser()
        self._cmd_args = cmd_parser.parse()
        self._pj_dir = os.path.join(env.BASE_DIR, "project", "spam")
        self._scenario_file = os.path.join(self._pj_dir, "scenario.yml")

    @pytest.fixture(autouse=True)
    def setup_resource(self):
        os.makedirs(self._pj_dir)
        yield "test in progress"
        shutil.rmtree(self._pj_dir)

    def test_add_system_path(self):
        runner = ScenarioRunner(self._cmd_args)
        runner.add_system_path()
        includes_spam = any("project/spam" in p for p in sys.path)
        self.assertTrue(includes_spam)

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

        with open(self._scenario_file, "w") as f:
            f.write(yaml.dump(test_data, default_flow_style=False))

        runner = ScenarioRunner(self._cmd_args)
        runner.create_scenario_queue()

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
        with open(self._scenario_file, "w") as f:
            f.write(yaml.dump(test_data, default_flow_style=False))

        runner = ScenarioRunner(self._cmd_args)
        runner.execute_scenario()

    def test_run_ng(self):
        with pytest.raises(FileNotFound):
            from cliboa.client import run

            run()
