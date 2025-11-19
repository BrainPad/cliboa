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
from cliboa.core.factory import _get_scenario_loader_class
from cliboa.core.loader import _ScenarioLoader
from cliboa.core.model import ScenarioModel
from cliboa.util.base import _BaseObject


class ScenarioParser(_BaseObject):
    """
    Base class of scenario file parser
    """

    def __init__(self, pj_scenario_file: str, cmn_scenario_file: str, scenario_format: str):
        super().__init__()
        self._pj_scenario_file = pj_scenario_file
        self._cmn_scenario_file = cmn_scenario_file
        self._loader_class: _ScenarioLoader = _get_scenario_loader_class(scenario_format)

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
