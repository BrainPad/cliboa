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

import yaml

from cliboa.client import CommandArgumentParser, ScenarioRunner
from cliboa.conf import env


class TestCommandArgumentParser(object):
    def setup_method(self, method):
        sys.argv.clear()
        sys.argv.append("spam")
        sys.argv.append("spam")

    def test_parse(self):
        cmd_parser = CommandArgumentParser()
        cmd_args = cmd_parser.parse()
        assert cmd_args.project_name == "spam"


class TestScenarioRunner(object):
    def setup_method(self, method):
        sys.argv.clear()
        sys.argv.append("spam")
        sys.argv.append("spam")
        cmd_parser = CommandArgumentParser()
        self._cmd_args = cmd_parser.parse()
        self._pj_dir = os.path.join(env.BASE_DIR, "project", "spam")
        self._scenario_file = os.path.join(self._pj_dir, "scenario.yml")

    def test_add_system_path(self):
        runner = ScenarioRunner(self._cmd_args)
        runner.add_system_path()
        includes_spam = any("project/spam" in p for p in sys.path)
        assert includes_spam is True

    def test_create_scenario_queue_ok(self):
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

        is_completed_queue_creation = True
        try:
            runner = ScenarioRunner(self._cmd_args)
            runner.create_scenario_queue()
        except Exception:
            is_completed_queue_creation = False
        else:
            shutil.rmtree(self._pj_dir)
        assert is_completed_queue_creation is True
