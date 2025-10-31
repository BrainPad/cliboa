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
import configparser
import json
import os
import re

from cliboa import state
from cliboa.conf import env
from cliboa.listener.base import BaseStepListener
from cliboa.scenario.base import BaseStep


class StepStatusListener(BaseStepListener):
    """
    This listener is only for logging.
    By default, Cliboa implements StepStatusListener in all steps.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        mask = pattern = None
        # TODO: refactor
        path = os.path.join(env.BASE_DIR, "conf", "cliboa.ini")
        if os.path.exists(path):
            try:
                conf = configparser.ConfigParser()
                conf.read(path, encoding="utf-8")
                mask = conf.get("logging", "mask")
                pattern = re.compile(mask)
            except Exception as e:
                self._logger.warning(e)
        self._pattern = pattern

    def before(self, step: BaseStep) -> None:
        state.set(step.__class__.__name__)
        props_dict = {}
        for k, v in step.__dict__.items():
            if self._pattern is not None and self._pattern.search(k):
                props_dict[k] = "****"
            else:
                props_dict[k] = v
        self._logger.info(
            "Step properties: %s" % json.dumps(props_dict, ensure_ascii=False, default=str)
        )
        self._logger.info("Start step execution. %s" % step.__class__.__name__)

    def after(self, step: BaseStep) -> None:
        self._logger.info("Finish step execution. %s" % step.__class__.__name__)

    def completion(self, step: BaseStep) -> None:
        self._logger.info("Complete step execution. %s" % step.__class__.__name__)
