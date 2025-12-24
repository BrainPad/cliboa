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
import re

from pydantic import BaseModel

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
        mask = partial_mask = pattern = partial_pattern = None
        partial_num = 3
        try:
            mask = env.get("LOGGING_MASK", ".*password.*|.*secret_key.*")
            partial_mask = env.get("LOGGING_PARTIAL_MASK", ".*access_key.*|.*token.*")
            partial_num = int(env.get("LOGGING_PARTIAL_NUM", 3))
            pattern = re.compile(mask)
            partial_pattern = re.compile(partial_mask)
        except Exception as e:
            self.logger.warning(e)
        self._pattern = pattern
        self._partial_pattern = partial_pattern
        self._partial_num = partial_num

    def before(self, step: BaseStep) -> None:
        state.set(step.__class__.__name__)
        props_dict = {}
        props_values = step.__dict__.copy()
        if isinstance(props_values.get("_args"), BaseModel):
            args = props_values.pop("_args")
            props_values.update(args.model_dump())
        for k, v in props_values.items():
            if k in (
                "_di_map",
                "_di_kwargs",
                "_logger",
                "_parent",
                "_args",
                "_deprecated_warn_log",
            ):
                continue
            if v is not None and self._pattern is not None and self._pattern.search(k):
                props_dict[k] = "****"
            elif (
                v is not None
                and self._partial_pattern is not None
                and self._partial_pattern.search(k)
            ):
                orig_value = str(v)
                if len(orig_value) > self._partial_num * 2:
                    props_dict[k] = (
                        orig_value[: self._partial_num] + "****" + orig_value[-self._partial_num :]
                    )
                elif len(orig_value) > self._partial_num:
                    props_dict[k] = "****" + orig_value[-self._partial_num :]
                else:
                    props_dict[k] = "****"
            else:
                props_dict[k] = v
        self.logger.info(
            "Step properties: %s" % json.dumps(props_dict, ensure_ascii=False, default=str)
        )
        self.logger.info("Start step execution. %s" % step.__class__.__name__)

    def after(self, step: BaseStep) -> None:
        self.logger.info("Finish step execution. %s" % step.__class__.__name__)

    def completion(self, step: BaseStep) -> None:
        self.logger.info("Complete step execution. %s" % step.__class__.__name__)
