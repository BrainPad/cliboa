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
import os
from importlib import import_module
from typing import Any

from cliboa.conf import env
from cliboa.core.loader import _JsonScenarioLoader, _ScenarioLoader, _YamlScenarioLoader
from cliboa.listener.base import BaseListener
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
    """

    def __init__(self, project_name: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self._project_name = project_name

        self._env = self._resolve("env", env)

        self._prj_scenario_dir_name = self._env.get(
            "PROJECT_SCENARIO_DIR_NAME", self._env.get("SCENARIO_DIR_NAME")
        )
        self._cmn_classes = self._env.COMMON_CUSTOM_CLASSES
        self._prj_classes = self._env.PROJECT_CUSTOM_CLASSES
        self._cmn_root_paths = self._env.get("COMMON_CUSTOM_ROOT_PATHS")
        if self._cmn_root_paths is None:
            self._cmn_root_paths = [self._restore_v2_cmn_root_path()]
        self._prj_root_paths = self._env.get("PROJECT_CUSTOM_ROOT_PATHS")
        if self._prj_root_paths is None:
            self._prj_root_paths = [self._restore_v2_prj_root_path()]

    def _restore_v2_cmn_root_path(self) -> str:
        """
        Restore v2 common root path for backward compability.
        """
        base_dir = self._env.get("BASE_DIR")
        common_scenario_dir_name = self._env.get("COMMON_SCENARIO_DIR")
        if (
            base_dir is not None
            and common_scenario_dir_name is not None
            and common_scenario_dir_name.startswith(base_dir)
        ):
            common_scenario_root_dir = common_scenario_dir_name[len(base_dir) + len(os.sep) :]
            return ".".join(common_scenario_root_dir.split(os.sep))
        else:
            return "common.scenario"

    def _restore_v2_prj_root_path(self) -> str:
        """
        Restore v2 project root path for backward compability.
        """
        base_dir = self._env.get("BASE_DIR")
        prj_dir_name = self._env.get("PROJECT_DIR")
        if base_dir is not None and prj_dir_name is not None and prj_dir_name.startswith(base_dir):
            prj_root_dir = prj_dir_name[len(base_dir) + len(os.sep) :]
            return ".".join(prj_root_dir.split(os.sep))
        else:
            return "project"

    def _import_custom_cls(
        self,
        mod_name: str,
        sliced_paths: list[str],
        root_paths: list[str],
        is_prj: bool,
    ) -> type[Any]:
        errors: list[str] = []
        for root_path in root_paths:
            try:
                if root_path == "":
                    import_root_paths = []
                else:
                    import_root_paths = root_path.split(".")
                if is_prj:
                    import_root_paths.append(self._project_name)
                    if self._prj_scenario_dir_name is not None:
                        import_root_paths.append(self._prj_scenario_dir_name)
                import_root_paths += sliced_paths
                root = ".".join(import_root_paths)
                return getattr(import_module(root), mod_name)
            except (ImportError, AttributeError) as e:
                errors.append(str(e))
        cls_name = ".".join(sliced_paths) + "." + mod_name
        raise ImportError(f"Failed to cliboa import {cls_name}:\n" + "\n".join(errors))

    def _get_custom_cls(
        self, cls_name: str, root_paths: list[str], custom_classes: list[str], is_prj: bool = False
    ) -> type[Any] | None:
        for custom_class_path in custom_classes:
            sliced_paths = custom_class_path.split(".")
            mod_name = sliced_paths.pop()
            if mod_name != cls_name:
                continue
            return self._import_custom_cls(mod_name, sliced_paths, root_paths, is_prj)
        return None

    def create(self, cls_name: str, **kwargs) -> object:
        """
        Create cliboa step|listener instance.
        """
        try:
            cls = self._get_custom_cls(cls_name, self._cmn_root_paths, self._cmn_classes)
            if cls is None and self._project_name:
                cls = self._get_custom_cls(
                    cls_name, self._prj_root_paths, self._prj_classes, is_prj=True
                )
            if cls is None:
                cls = globals()[cls_name]
            instance = cls(**kwargs)
            if not isinstance(
                instance,
                (
                    BaseStep,  # noqa
                    BaseListener,
                ),
            ):
                self._logger.warning(
                    f"{instance.__class__.__name__} does not inherit from "
                    "cliboa.scenario.base.BaseStep or cliboa.listener.base.BaseListener"
                )
            return instance
        except Exception:
            self._logger.exception(f"Failed to create an instance of {cls_name}")
            raise InvalidScenarioClass(f"Invalid scenario class '{cls_name}'")
