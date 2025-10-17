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

from cliboa.core.validator import EssentialKeys
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

    def parse(self) -> list[dict]:
        """
        Parse scenario file
        """
        self._logger.info("Start to parse scenario file.")

        pj_top_dict = self._loader_class(self._pj_scenario_file, True)()
        self._valid_scenario(pj_top_dict)

        cmn_top_dict = self._loader_class(self._cmn_scenario_file, False)()
        if cmn_top_dict:
            self._valid_scenario(cmn_top_dict)
            scenario_list = self._merge_scenario(pj_top_dict["scenario"], cmn_top_dict["scenario"])
        else:
            scenario_list = pj_top_dict["scenario"]

        self._logger.info("Finish to parse scenario file.")
        return scenario_list

    def _valid_scenario(self, top_dict: dict) -> None:
        """
        validate instance type and essential key in scenario.yml
        """
        scenario_list = top_dict.get("scenario")
        if not scenario_list:
            raise ScenarioFileInvalid(
                "scenario file is invalid. 'scenario' key does not exist, or 'scenario' key exists but content under 'scenario' key does not exist."  # noqa
            )
        valid = EssentialKeys(scenario_list)
        valid()

    def _merge_scenario(self, pj_list: list, cmn_list: list) -> list:
        """
        Merge project scenario.yml and common scenario.yml.
        If the same class specification exists,
        scenario.yml of projet is taken priority.
        """
        for pj_dict in pj_list:
            # If same class exists, merge arguments
            if pj_dict.get("parallel"):
                for row in pj_dict.get("parallel"):
                    self._merge(row, cmn_list)
            elif pj_dict.get("parallel_with_config"):
                steps = pj_dict.get("parallel_with_config").get("steps")
                for row in steps:
                    self._merge(row, cmn_list)
            else:
                self._merge(pj_dict, cmn_list)

        return pj_list

    def _merge(self, pj_dict: dict, cmn_list: list[dict]) -> None:
        cmn_dict_in_list = [d for d in cmn_list if d.get("class") == pj_dict.get("class")]
        if not cmn_dict_in_list:
            return

        pj_cls_attrs = pj_dict.get("arguments", "")
        cmn_cls_attrs = cmn_dict_in_list[0].get("arguments")

        # Merge arguments
        if pj_cls_attrs and cmn_cls_attrs:
            pj_cls_attrs = dict(cmn_cls_attrs, **pj_cls_attrs)
        elif not pj_cls_attrs and cmn_cls_attrs:
            pj_cls_attrs = cmn_cls_attrs
        pj_dict["arguments"] = pj_cls_attrs


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
