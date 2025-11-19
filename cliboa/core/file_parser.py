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

from cliboa.core.model import ScenarioModel
from cliboa.util.base import _BaseObject
from cliboa.util.exception import FileNotFound, InvalidFormat, ScenarioFileInvalid


class ScenarioParser(_BaseObject):
    """
    Base class of scenario file parser
    """

    def __init__(self, pj_scenario_file: str, cmn_scenario_file: str, scenario_format: str):
        super().__init__()
        self._pj_scenario_file = pj_scenario_file
        self._cmn_scenario_file = cmn_scenario_file
        if scenario_format == "yaml":
            loader_class = _YamlScenarioLoader
        elif scenario_format == "json":
            loader_class = _JsonScenarioLoader
        else:
            raise InvalidFormat(f"scenario format '{scenario_format}' is invalid.")
        self._loader_class: _ScenarioLoader = loader_class

    def parse(self) -> ScenarioModel:
        """
        Parse scenario file
        """
        self._logger.info("Start to parse scenario file.")

        pj_top_dict = self._loader_class(self._pj_scenario_file, True)()
        pj_scenario = ScenarioModel.model_validate(pj_top_dict)

        cmn_top_dict = self._loader_class(self._cmn_scenario_file, False)()
        if cmn_top_dict:
            cmn_scenario = ScenarioModel.model_validate(cmn_top_dict)
            pj_scenario.merge(cmn_scenario)

        self._logger.info("Finish to parse scenario file.")
        return pj_scenario


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
