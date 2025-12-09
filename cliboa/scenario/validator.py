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
from cliboa.util.base import _BaseObject
from cliboa.util.exception import InvalidParameter


class EssentialParameters(_BaseObject):
    """
    Validation for the essential parameters of step class
    """

    def __init__(self, cls_name, param_list, **kwargs):
        """
        Args:
            cls_name: class name which has validation target parameters
            param_list: list of validation target parameters
        """
        super().__init__(**kwargs)
        self._cls_name = cls_name
        self._param_list = param_list

    def __call__(self):
        for p in self._param_list:
            if not p:
                raise InvalidParameter(
                    "The essential parameter is not specified in %s." % self._cls_name
                )
