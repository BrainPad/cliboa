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
from cliboa.scenario.file import FileRead as _FR
from cliboa.util.base import _warn_deprecated

_warn_deprecated(
    "cliboa.scenario.load.file",
    "3.0",
    "4.0",
    "cliboa.scenario.file",
)


class FileRead(_FR):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger.warning(
            _warn_deprecated(
                "cliboa.scenario.load.file.FileRead",
                "3.0",
                "4.0",
                "cliboa.scenario.file.FileRead",
            )
        )
