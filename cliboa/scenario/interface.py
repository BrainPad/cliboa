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


class IParentStep(ABC):
    """
    Interface for the runtime parent class of a BaseStep subclass (not the inheritance parent).
    """

    @abstractmethod
    def get_symbol_arguments(self) -> dict[str, Any]:
        """
        Returns the arguments dict for the step specified by symbol
        (variables are already transformed).
        Returns {} if symbol is not specified, and no exceptions are raised.
        """
        raise NotImplementedError()

    @abstractmethod
    def put_to_context(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_from_context(self, step_name: str | None = None) -> Any:
        raise NotImplementedError()

    @property
    @abstractmethod
    def step_name(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def symbol_name(self) -> str | None:
        raise NotImplementedError()
