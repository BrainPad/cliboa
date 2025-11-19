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
from functools import partial

from cliboa.listener.base import BaseStepListener
from cliboa.scenario.base import BaseStep
from cliboa.util.base import _warn_deprecated

_warn_deprecated_step_listener = partial(
    _warn_deprecated,
    "cliboa.core.listener.StepListener",
    "cliboa.listener.base.BaseStepListener",
    "3.0",
)


class StepListener(BaseStepListener):
    """
    for v2 backward compatible
    """

    def __init__(self, *args, **kwargs):
        _warn_deprecated_step_listener()
        super().__init__(*args, **kwargs)

    def before(self, step: BaseStep) -> None:
        if hasattr(self, "before_step"):
            _warn_deprecated_step_listener()
            self.before_step(step)

    def after(self, step: BaseStep) -> None:
        if hasattr(self, "after_step"):
            _warn_deprecated_step_listener()
            self.after_step(step)

    def error(self, step: BaseStep, e: Exception) -> None:
        if hasattr(self, "error_step"):
            _warn_deprecated_step_listener()
            self.error_step(step)

    def completion(self, step: BaseStep) -> None:
        if hasattr(self, "after_completion"):
            _warn_deprecated_step_listener()
            self.after_completion(step)
