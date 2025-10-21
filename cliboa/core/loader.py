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
from abc import abstractmethod
from collections import OrderedDict

import yaml

from cliboa.util.base import _BaseObject
from cliboa.util.exception import FileNotFound, ScenarioFileInvalid


class _ScenarioLoader(_BaseObject):
    def __init__(self, scenario_file: str, is_required: bool = False):
        super().__init__()
        self._scenario_file = scenario_file
        if is_required and not self._exists():
            raise FileNotFound("File %s does not exist" % self._scenario_file)

    def _exists(self) -> bool:
        return os.path.isfile(self._scenario_file)

    def __call__(self) -> dict | None:
        """
        Load scenario file and return dict or None.
        """
        if not self._exists():
            return None
        top_dict = self._load()
        if not isinstance(top_dict, dict):
            raise ScenarioFileInvalid(
                f"scenario file {self._scenario_file} is invalid. Check file format."
            )
        return top_dict

    @abstractmethod
    def _load(self) -> dict:
        raise NotImplementedError()


class _YamlScenarioLoader(_ScenarioLoader):
    def _load(self) -> dict:
        with open(self._scenario_file, "r") as f:
            return yaml.safe_load(f)


class _JsonScenarioLoader(_ScenarioLoader):
    def _load(self) -> dict:
        with open(self._scenario_file, "r") as f:
            return json.load(f, object_pairs_hook=OrderedDict)
