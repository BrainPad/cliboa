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
from abc import ABC, abstractmethod
from typing import Any


class _IExecute(ABC):
    """
    Interface of executable step instance.
    """

    @abstractmethod
    def execute(self) -> int | None:
        pass


class _IContext(ABC):
    """
    Interface of context
    """

    @abstractmethod
    def put(self, key: str, value: Any) -> None:
        pass

    @abstractmethod
    def get(self, key: str) -> Any:
        pass
