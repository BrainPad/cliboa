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
from datetime import datetime, timedelta

import pytest
import yaml

from cliboa.client import CommandArgumentParser
from cliboa.conf import env
from cliboa.core.manager import JsonScenarioManager, YamlScenarioManager
from cliboa.core.scenario_queue import ScenarioQueue
from cliboa.test import BaseCliboaTest
from cliboa.util.exception import ScenarioFileInvalid
from cliboa.util.parallel_with_config import ParallelWithConfig


class TestYamlScenarioManager(BaseCliboaTest):
    def setUp(self):
        cmd_parser = CommandArgumentParser()
        sys.argv.clear()
        sys.argv.append("spam")
        sys.argv.append("spam")
        self._cmd_args = cmd_parser.parse()
        self._pj_dir = os.path.join(env.PROJECT_DIR, "spam")
        os.makedirs(self._pj_dir, exist_ok=True)
        self._pj_scenario_file = os.path.join(self._pj_dir, "scenario.yml")

    def tearDown(self):
        shutil.rmtree(self._pj_dir, ignore_errors=True)

    def _create_scenario_file(self, data):
        with open(self._pj_scenario_file, mode="w", encoding="utf-8") as f:
            f.write(yaml.dump(data, default_flow_style=False))

    def test_create_scenario_queue_ok(self):
        """
        Valid scenario.yml
        """
        pj_yaml_dict = {
            "scenario": [
                {"step": "sample_step", "class": "SampleStep", "arguments": {"retry_count": 10}}
            ]
        }
        self._create_scenario_file(pj_yaml_dict)

        manager = YamlScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        instance = instances[0]
        assert instance._step == "sample_step"
        assert instance._retry_count == 10

    def test_create_scenario_queue_ok_parallel(self):
        """
        Valid scenario.yml
        """
        pj_yaml_dict = {
            "scenario": [
                {"multi_process_count": 3},
                {"force_continue": True},
                {
                    "parallel": [
                        {
                            "step": "sample_step_1",
                            "class": "SampleStep",
                            "arguments": {"retry_count": 1},
                        },
                        {
                            "step": "sample_step_2",
                            "class": "SampleStep",
                            "arguments": {"retry_count": 2},
                        },
                    ]
                },
            ]
        }
        self._create_scenario_file(pj_yaml_dict)

        manager = YamlScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        assert len(instances) == 2
        assert ScenarioQueue.step_queue.multi_proc_cnt == 3
        assert ScenarioQueue.step_queue.force_continue is True
        for i, instance in enumerate(instances):
            if i == 0:
                assert instance._step == "sample_step_1"
                assert instance._retry_count == 1
            elif i == 1:
                assert instance._step == "sample_step_2"
                assert instance._retry_count == 2

    def test_create_scenario_queue_ok_parallel_with_config(self):
        """
        Valid scenario.yml
        """
        pj_yaml_dict = {
            "scenario": [
                {"multi_process_count": 3},
                {"force_continue": True},
                {
                    "parallel_with_config": {
                        "config": {"multi_process_count": 3},
                        "steps": [
                            {
                                "step": "sample_step_1",
                                "class": "SampleStep",
                                "arguments": {"retry_count": 1},
                            },
                            {
                                "step": "sample_step_2",
                                "class": "SampleStep",
                                "arguments": {"retry_count": 2},
                            },
                        ],
                    }
                },
            ]
        }
        self._create_scenario_file(pj_yaml_dict)

        manager = YamlScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()

        assert len(instances) == 1
        assert isinstance(instances[0], ParallelWithConfig)
        assert ScenarioQueue.step_queue.multi_proc_cnt == 3
        assert ScenarioQueue.step_queue.force_continue is True
        print(instances)
        assert instances[0].config["multi_process_count"] == 3
        for i, instance in enumerate(instances[0].steps):
            if i == 0:
                assert instance._step == "sample_step_1"
                assert instance._retry_count == 1
            elif i == 1:
                assert instance._step == "sample_step_2"
                assert instance._retry_count == 2

    def test_create_scenario_queue_ok_with_no_args(self):
        """
        Valid scenario.yml with no arguments
        """
        pj_yaml_dict = {"scenario": [{"step": "sample_step", "class": "SampleStep"}]}
        self._create_scenario_file(pj_yaml_dict)

        manager = YamlScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        instance = instances[0]
        assert instance._step == "sample_step"
        assert instance._retry_count == 3

    def test_create_scenario_queue_ok_with_vars(self):
        """
        Valid scenario.yml with {{ vars }}
        """
        pj_yaml_dict = {
            "scenario": [
                {
                    "step": "sample_step",
                    "class": "SampleStep",
                    "arguments": {
                        "memo": "foo_{{ today }}.csv",
                        "with_vars": {"today": "date '+%Y%m%d'"},
                    },
                }
            ]
        }
        self._create_scenario_file(pj_yaml_dict)

        manager = YamlScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        instance = instances[0]

        today = datetime.now().strftime("%Y%m%d")
        assert instance._step == "sample_step"
        assert instance._memo == "foo_%s.csv" % today

    def test_create_scenario_queue_ok_with_vars_plural(self):
        """
        Valid scenario.yml with {{ vars }}
        """
        pj_yaml_dict = {
            "scenario": [
                {
                    "step": "sample_step",
                    "class": "SampleStep",
                    "arguments": {
                        "memo": "foo_{{ yesterday }}_{{ today }}.csv",
                        "with_vars": {
                            "today": "date '+%Y%m%d'",
                            "yesterday": "date '+%Y%m%d' --date='1 day ago'",
                        },
                    },
                }
            ]
        }
        self._create_scenario_file(pj_yaml_dict)

        manager = YamlScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        instance = instances[0]

        today = datetime.now().strftime("%Y%m%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        assert instance._step == "sample_step"
        assert instance._memo == "foo_%s_%s.csv" % (yesterday, today)

    def test_create_scenario_queue_ok_with_vars_list(self):
        """
        Valid scenario.yml with {{ vars }}
        """
        pj_yaml_dict = {
            "scenario": [
                {
                    "step": "sample_step",
                    "class": "SampleStep",
                    "arguments": {
                        "memo": ["foo_{{ today }}.csv", "foo_{{ yesterday }}.csv"],
                        "with_vars": {
                            "today": "date '+%Y%m%d'",
                            "yesterday": "date '+%Y%m%d' --date='1 day ago'",
                        },
                    },
                }
            ]
        }
        self._create_scenario_file(pj_yaml_dict)

        manager = YamlScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        instance = instances[0]

        today = datetime.now().strftime("%Y%m%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        assert instance._step == "sample_step"
        assert type(instance._memo) == list
        assert len(instance._memo) == 2
        for i, row in enumerate(instance._memo):
            if i == 0:
                assert row == "foo_%s.csv" % today
            elif i == 1:
                assert row == "foo_%s.csv" % yesterday

    def test_create_scenario_queue_ok_with_vars_dict(self):
        """
        Valid scenario.yml with {{ vars }}
        """
        pj_yaml_dict = {
            "scenario": [
                {
                    "step": "sample_step",
                    "class": "SampleStep",
                    "arguments": {
                        "memo": {"one": "foo_{{ today }}.csv", "two": "foo_{{ yesterday }}.csv"},
                        "with_vars": {
                            "today": "date '+%Y%m%d'",
                            "yesterday": "date '+%Y%m%d' --date='1 day ago'",
                        },
                    },
                }
            ]
        }
        self._create_scenario_file(pj_yaml_dict)

        manager = YamlScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        instance = instances[0]

        today = datetime.now().strftime("%Y%m%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        assert instance._step == "sample_step"
        assert type(instance._memo) == dict
        for k, v in instance._memo.items():
            if k == "one":
                assert v == "foo_%s.csv" % today
            elif k == "two":
                assert v == "foo_%s.csv" % yesterday

    def test_create_scenario_queue_ok_with_vars_complicated(self):
        """
        Valid scenario.yml with {{ vars }}
        """
        pj_yaml_dict = {
            "scenario": [
                {
                    "step": "sample_step",
                    "class": "SampleStep",
                    "arguments": {
                        "memo": [
                            {"one": "foo_{{ today }}.csv", "two": "foo_{{ yesterday }}.csv"},
                            {"one": "foo_{{ today }}.txt", "two": "foo_{{ yesterday }}.txt"},
                        ],
                        "with_vars": {
                            "today": "date '+%Y%m%d'",
                            "yesterday": "date '+%Y%m%d' --date='1 day ago'",
                        },
                    },
                }
            ]
        }
        self._create_scenario_file(pj_yaml_dict)

        manager = YamlScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        instance = instances[0]

        today = datetime.now().strftime("%Y%m%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        assert instance._step == "sample_step"
        assert type(instance._memo) == list
        assert len(instance._memo) == 2
        for i, row in enumerate(instance._memo):
            if i == 0:
                ext = "csv"
            elif i == 1:
                ext = "txt"
            for k, v in row.items():
                if k == "one":
                    assert v == "foo_%s.%s" % (today, ext)
                elif k == "two":
                    assert v == "foo_%s.%s" % (yesterday, ext)

    def test_create_scenario_queue_ng(self):
        """
        Invalid scenario.yml

        scenario:
          - arguments
          - spam
        """
        pj_yaml_dict = {"scenario": ["arguments", "spam"]}
        self._create_scenario_file(pj_yaml_dict)

        with pytest.raises(AttributeError) as excinfo:
            manager = YamlScenarioManager(self._cmd_args)
            manager.create_scenario_queue()
        assert "object has no attribute" in str(excinfo.value)

    def test_create_scenario_queue_with_no_list_ng(self):
        """
        Invalid scenario.yml

        scenario:
          arguments:
            spam: test
        """
        pj_yaml_dict = {"scenario": {"arguments": {"spam": "test"}}}
        self._create_scenario_file(pj_yaml_dict)

        with pytest.raises(ScenarioFileInvalid) as excinfo:
            manager = YamlScenarioManager(self._cmd_args)
            manager.create_scenario_queue()
        assert "invalid" in str(excinfo.value)

    def test_replace_environment_values(self):
        os.environ["CLIBOA_TEST_ENV"] = "ABC"
        pj_yaml_dict = {
            "scenario": [
                {
                    "step": "sample_step",
                    "class": "SampleStep",
                    "arguments": {"memo": "foo_{{ env.CLIBOA_TEST_ENV }}_bar",},
                }
            ]
        }
        self._create_scenario_file(pj_yaml_dict)

        manager = YamlScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        instance = instances[0]

        assert instance._step == "sample_step"
        assert instance._memo == "foo_ABC_bar"


class TestJsonScenarioManager(BaseCliboaTest):
    def setUp(self):
        cmd_parser = CommandArgumentParser()
        sys.argv.clear()
        sys.argv.append("project_name")
        sys.argv.append("spam")
        sys.argv.append("--format")
        sys.argv.append("json")
        self._cmd_args = cmd_parser.parse()
        self._pj_dir = os.path.join(env.PROJECT_DIR, "spam")
        os.makedirs(self._pj_dir, exist_ok=True)
        self._pj_scenario_file = os.path.join(self._pj_dir, "scenario.json")

    def tearDown(self):
        shutil.rmtree(self._pj_dir, ignore_errors=True)

    def _create_scenario_file(self, data):
        with open(self._pj_scenario_file, mode="w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

    def test_create_scenario_queue_ok(self):
        """
        Valid scenario.json
        """
        pj_json_dict = {
            "scenario": [
                {"step": "sample_step", "class": "SampleStep", "arguments": {"retry_count": 10}}
            ]
        }
        self._create_scenario_file(pj_json_dict)

        manager = JsonScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        instance = instances[0]
        assert instance._step == "sample_step"
        assert instance._retry_count == 10

    def test_create_scenario_queue_ok_parallel(self):
        """
        Valid scenario.json
        """
        pj_json_dict = {
            "scenario": [
                {"multi_process_count": 3},
                {"force_continue": True},
                {
                    "parallel": [
                        {
                            "step": "sample_step_1",
                            "class": "SampleStep",
                            "arguments": {"retry_count": 1},
                        },
                        {
                            "step": "sample_step_2",
                            "class": "SampleStep",
                            "arguments": {"retry_count": 2},
                        },
                    ]
                },
            ]
        }
        self._create_scenario_file(pj_json_dict)

        manager = JsonScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        assert len(instances) == 2
        assert ScenarioQueue.step_queue.multi_proc_cnt == 3
        assert ScenarioQueue.step_queue.force_continue is True
        for i, instance in enumerate(instances):
            if i == 0:
                assert instance._step == "sample_step_1"
                assert instance._retry_count == 1
            elif i == 1:
                assert instance._step == "sample_step_2"
                assert instance._retry_count == 2

    def test_create_scenario_queue_ok_with_no_args(self):
        """
        Valid scenario.json with no arguments
        """
        pj_json_dict = {"scenario": [{"step": "sample_step", "class": "SampleStep"}]}
        self._create_scenario_file(pj_json_dict)

        manager = JsonScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        instance = instances[0]
        assert instance._step == "sample_step"
        assert instance._retry_count == 3

    def test_create_scenario_queue_ok_with_vars(self):
        """
        Valid scenario.json with {{ vars }}
        """
        pj_json_dict = {
            "scenario": [
                {
                    "step": "sample_step",
                    "class": "SampleStep",
                    "arguments": {
                        "memo": "foo_{{ today }}.csv",
                        "with_vars": {"today": "date '+%Y%m%d'"},
                    },
                }
            ]
        }
        self._create_scenario_file(pj_json_dict)

        manager = JsonScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        instance = instances[0]

        today = datetime.now().strftime("%Y%m%d")
        assert instance._step == "sample_step"
        assert instance._memo == "foo_%s.csv" % today

    def test_create_scenario_queue_ok_with_vars_plural(self):
        """
        Valid scenario.json with {{ vars }}
        """
        pj_json_dict = {
            "scenario": [
                {
                    "step": "sample_step",
                    "class": "SampleStep",
                    "arguments": {
                        "memo": "foo_{{ yesterday }}_{{ today }}.csv",
                        "with_vars": {
                            "today": "date '+%Y%m%d'",
                            "yesterday": "date '+%Y%m%d' --date='1 day ago'",
                        },
                    },
                }
            ]
        }
        self._create_scenario_file(pj_json_dict)

        manager = JsonScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        instance = instances[0]

        today = datetime.now().strftime("%Y%m%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        assert instance._step == "sample_step"
        assert instance._memo == "foo_%s_%s.csv" % (yesterday, today)

    def test_create_scenario_queue_ok_with_vars_list(self):
        """
        Valid scenario.json with {{ vars }}
        """
        pj_json_dict = {
            "scenario": [
                {
                    "step": "sample_step",
                    "class": "SampleStep",
                    "arguments": {
                        "memo": ["foo_{{ today }}.csv", "foo_{{ yesterday }}.csv"],
                        "with_vars": {
                            "today": "date '+%Y%m%d'",
                            "yesterday": "date '+%Y%m%d' --date='1 day ago'",
                        },
                    },
                }
            ]
        }
        self._create_scenario_file(pj_json_dict)

        manager = JsonScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        instance = instances[0]

        today = datetime.now().strftime("%Y%m%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        assert instance._step == "sample_step"
        assert type(instance._memo) == list
        assert len(instance._memo) == 2
        for i, row in enumerate(instance._memo):
            if i == 0:
                assert row == "foo_%s.csv" % today
            elif i == 1:
                assert row == "foo_%s.csv" % yesterday

    def test_create_scenario_queue_ok_with_vars_dict(self):
        """
        Valid scenario.json with {{ vars }}
        """
        pj_json_dict = {
            "scenario": [
                {
                    "step": "sample_step",
                    "class": "SampleStep",
                    "arguments": {
                        "memo": {"one": "foo_{{ today }}.csv", "two": "foo_{{ yesterday }}.csv"},
                        "with_vars": {
                            "today": "date '+%Y%m%d'",
                            "yesterday": "date '+%Y%m%d' --date='1 day ago'",
                        },
                    },
                }
            ]
        }
        self._create_scenario_file(pj_json_dict)

        manager = JsonScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        instance = instances[0]

        today = datetime.now().strftime("%Y%m%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        assert instance._step == "sample_step"
        assert type(instance._memo) == dict
        for k, v in instance._memo.items():
            if k == "one":
                assert v == "foo_%s.csv" % today
            elif k == "two":
                assert v == "foo_%s.csv" % yesterday

    def test_create_scenario_queue_ok_with_vars_complicated(self):
        """
        Valid scenario.json with {{ vars }}
        """
        pj_json_dict = {
            "scenario": [
                {
                    "step": "sample_step",
                    "class": "SampleStep",
                    "arguments": {
                        "memo": [
                            {"one": "foo_{{ today }}.csv", "two": "foo_{{ yesterday }}.csv"},
                            {"one": "foo_{{ today }}.txt", "two": "foo_{{ yesterday }}.txt"},
                        ],
                        "with_vars": {
                            "today": "date '+%Y%m%d'",
                            "yesterday": "date '+%Y%m%d' --date='1 day ago'",
                        },
                    },
                }
            ]
        }
        self._create_scenario_file(pj_json_dict)

        manager = JsonScenarioManager(self._cmd_args)
        manager.create_scenario_queue()
        instances = ScenarioQueue.step_queue.pop()
        instance = instances[0]

        today = datetime.now().strftime("%Y%m%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        assert instance._step == "sample_step"
        assert type(instance._memo) == list
        assert len(instance._memo) == 2
        for i, row in enumerate(instance._memo):
            if i == 0:
                ext = "csv"
            elif i == 1:
                ext = "txt"
            for k, v in row.items():
                if k == "one":
                    assert v == "foo_%s.%s" % (today, ext)
                elif k == "two":
                    assert v == "foo_%s.%s" % (yesterday, ext)

    def test_create_scenario_queue_ng(self):
        """
        Invalid scenario.json

        scenario:
          - arguments
          - spam
        """
        pj_json_dict = {"scenario": ["arguments", "spam"]}
        self._create_scenario_file(pj_json_dict)

        with pytest.raises(AttributeError) as excinfo:
            manager = JsonScenarioManager(self._cmd_args)
            manager.create_scenario_queue()
        assert "object has no attribute" in str(excinfo.value)

    def test_create_scenario_queue_with_no_list_ng(self):
        """
        Invalid scenario.json

        scenario:
          arguments:
            spam: test
        """
        pj_json_dict = {"scenario": {"arguments": {"spam": "test"}}}
        self._create_scenario_file(pj_json_dict)

        with pytest.raises(ScenarioFileInvalid) as excinfo:
            manager = JsonScenarioManager(self._cmd_args)
            manager.create_scenario_queue()
        assert "invalid" in str(excinfo.value)
