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
from abc import abstractmethod

import yaml

from cliboa.core.validator import EssentialKeys, ScenarioYamlKey, ScenarioYamlType  # noqa
from cliboa.util.lisboa_log import LisboaLog


class ScenarioParser(object):
    """
    Base class of scnario file parser
    """

    def __init__(self, pj_scenario_file, cmn_scenario_file):
        self._logger = LisboaLog.get_logger(__name__)
        self._pj_scenario_file = pj_scenario_file
        self._cmn_scenario_file = cmn_scenario_file

    @abstractmethod
    def parse(self):
        """
        Parse scenario file
        """


class YamlScenarioParser(ScenarioParser):
    """
    scenario.yml parser
    """

    def parse(self):
        self._logger.info("Start to parse scenario.yml")
        pj_f = open(self._pj_scenario_file, "r")
        pj_yaml_dict = yaml.safe_load(pj_f)

        self.__valid_scenario_yaml(pj_yaml_dict)
        exists_cmn_scenario_file = os.path.isfile(self._cmn_scenario_file)
        if exists_cmn_scenario_file:
            cmn_f = open(self._cmn_scenario_file, "r")
            cmn_yaml_dict = yaml.safe_load(cmn_f)
            self.__valid_scenario_yaml(cmn_yaml_dict)
            self.__exists_ess_keys(pj_yaml_dict["scenario"])
            self.__exists_ess_keys(cmn_yaml_dict["scenario"])
            yaml_list = self.__merge_scenario_yaml(
                pj_yaml_dict["scenario"], cmn_yaml_dict["scenario"]
            )
            cmn_f.close()
        else:
            yaml_list = pj_yaml_dict.get("scenario")

        self._logger.info("Finish to parse scenario.yml")
        pj_f.close()
        return yaml_list

    def __valid_scenario_yaml(self, yaml_dict):
        """
        validate instance type and essential key in scenario.yml
        """
        for scenario_yaml_valid in ["ScenarioYamlType", "ScenarioYamlKey"]:
            cls_name = globals()[scenario_yaml_valid](yaml_dict)
            cls_name()

    def __merge_scenario_yaml(self, pj_yaml_list, cmn_yaml_list):
        """
        Merge project scenrio.yml and common scenairo.yml.
        If the same class specification exists,
        scenario.yml of projet is taken priority.
        """
        for pj_yaml_dict in pj_yaml_list:
            # If same class exists, merge arguments
            cmn_yaml_dict_in_list = [
                d for d in cmn_yaml_list if d.get("class") == pj_yaml_dict.get("class")
            ]
            if not cmn_yaml_dict_in_list:
                continue

            pj_cls_attrs = pj_yaml_dict.get("arguments", "")
            cmn_cls_attrs = cmn_yaml_dict_in_list[0].get("arguments")

            # Merge arguments
            if pj_cls_attrs and cmn_cls_attrs:
                pj_cls_attrs = dict(cmn_cls_attrs, **pj_cls_attrs)
            elif not pj_cls_attrs and cmn_cls_attrs:
                pj_cls_attrs = cmn_cls_attrs
            pj_yaml_dict["arguments"] = pj_cls_attrs

        return pj_yaml_list

    def __exists_ess_keys(self, scenario_yaml_list):
        """
        Check if the essential keys exist in scenario.yml
        """
        valid = EssentialKeys(scenario_yaml_list)
        valid()


class JsonScenarioParser:
    """
    TODO: implement in the future
    """
