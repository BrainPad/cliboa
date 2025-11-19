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
from importlib import import_module
from typing import Tuple

from cliboa.conf import env
from cliboa.core.loader import _JsonScenarioLoader, _ScenarioLoader, _YamlScenarioLoader
from cliboa.scenario import *  # noqa
from cliboa.util.base import _BaseObject
from cliboa.util.exception import InvalidFormat, InvalidScenarioClass


def _get_scenario_loader_class(file_format: str) -> type[_ScenarioLoader]:
    """
    Create scenario loader instance
    """
    if file_format == "yaml":
        return _YamlScenarioLoader
    elif file_format == "json":
        return _JsonScenarioLoader
    else:
        raise InvalidFormat(f"scenario format '{file_format}' is invalid.")


class _CliboaFactory(_BaseObject):
    """
    Factory class to create cliboa step|listener instance
     - both cliboa's default class and user's custom class.

    Depends on envs: COMMON_CUSTOM_CLASSES, PROJECT_CUSTOM_CLASSES
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._custom_classes = env.COMMON_CUSTOM_CLASSES + env.PROJECT_CUSTOM_CLASSES

    def _describe_class(self, cls_name: str) -> Tuple[str, str] | None:
        """
        Returns a pair of module path and class name by given name.

        Args:
            cls_name: Class name

        Return (tuple):
            tuple:
                - module path
                - class name
        """
        module = None
        for c in self._custom_classes:
            s = c.split(".")
            if s[-1:][0] == cls_name:
                module = s
                break

        if module is None:
            return None

        root = ".".join(module[:-1])
        mod_name = module[-1:][0]

        return root, mod_name

    def _create_custom_instance(self, cls_name: str, **kwargs) -> object | None:
        """
        Import python module and create instance dynamically

        Return:
            Created instance.
            None: If cls_name was not found in the defined class list.
        """
        ret = self._describe_class(cls_name)
        if ret is None:
            return None
        (root, mod_name) = ret
        instance = getattr(import_module(root), mod_name)
        return instance(**kwargs)

    def create(self, cls_name: str, **kwargs) -> object:
        """
        Create cliboa step|listener instance.
        """
        try:
            instance = self._create_custom_instance(cls_name, **kwargs)
            if instance is None:
                cls = globals()[cls_name]
                instance = cls(**kwargs)
            return instance
        except Exception:
            self._logger.exception(f"Failed to create an instance of {cls_name}")
            raise InvalidScenarioClass(f"Invalid scenario class '{cls_name}'")


_cliboa_factory = _CliboaFactory()
