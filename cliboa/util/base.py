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
import inspect
import warnings
from abc import ABC
from typing import Type, TypeVar

from cliboa.util.exception import CliboaException
from cliboa.util.log import _get_logger

T = TypeVar("T")


class _BaseObject(ABC):
    def __init__(self, *args, **kwargs):
        try:
            super().__init__(*args, **kwargs)
        except TypeError as e:
            if "object.__init__()" in str(e):
                super().__init__()
            else:
                raise e
        self._di_map = {}
        self._di_kwargs = kwargs
        for k, v in kwargs.items():
            if k.startswith("di_"):
                key = k[3:]
                self._di_map[key] = v
        self._logger = self._resolve(
            "logger", _get_logger(self.__class__.__module__ + "." + self.__class__.__name__)
        )

    def _resolve_cls(self, key: str, default_cls: Type[T], *args, **kwargs) -> Type[T]:
        dependency = self._di_map.get(key, default_cls)
        if not inspect.isclass(dependency):
            raise CliboaException(f"Failed to resolve cliboa class. {key}:{type(dependency)}")
        return dependency

    def _resolve(self, key: str, default_cls: Type[T], *args, **kwargs) -> T:
        dependency = self._di_map.get(key, default_cls)
        if not inspect.isclass(dependency):
            return dependency
        merged_kwargs = self._di_kwargs | kwargs
        return dependency(*args, **merged_kwargs)


def _warn_deprecated(
    deprecated: str, instead: str | None = None, end_version: str | None = None, stacklevel: int = 3
) -> None:
    err_mes = f"{deprecated} is deprecated."
    if instead:
        err_mes += f" Use {instead} instead."
    if end_version:
        err_mes += f" It is no longer supported since version {end_version}."
    warnings.warn(
        err_mes,
        DeprecationWarning,
        stacklevel=stacklevel,
    )
