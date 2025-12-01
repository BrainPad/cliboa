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
from cliboa.scenario.validator import EssentialParameters as _ValidEP
from cliboa.util.base import _warn_deprecated

_warn_deprecated("cliboa.core.validator", "3.0", "4.0")


class EssentialParameters(_ValidEP):
    """
    DEPRECATED: Use cliboa.scenario.validator.EssentialParameters instead.
    This class will be removed in a future version (v3 or later).
    """

    def __init__(self, *args, **kwargs):
        _warn_deprecated(
            ".".join(("cliboa.core.validator", self.__class__.__name__)),
            "2.6.0beta",
            "4.0",
            ".".join(("cliboa.scenario.validator", self.__class__.__name__)),
        )
        super().__init__(*args, **kwargs)
