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
"""
Recipe expansion: replace ``RecipeStepModel`` entries in a validated ``ScenarioModel``.
"""

from os import path

from pydantic import ValidationError

from cliboa.core.loader import _ScenarioFormat
from cliboa.core.model import (
    ParallelStepModel,
    RecipeModel,
    RecipeStepModel,
    ScenarioModel,
    StepModel,
)
from cliboa.util.base import _BaseObject
from cliboa.util.exception import CliboaRuntimeError, InvalidParameter, ScenarioFileInvalid


class _RecipeExpander(_BaseObject):
    """
    Expand ``RecipeStepModel`` entries in a validated ``ScenarioModel``.
    """

    def __init__(
        self,
        recipe_dirs: list[str],
        scenario_format: _ScenarioFormat,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._recipe_dirs = list(recipe_dirs or [])
        self._loader_cls = scenario_format.loader_cls()
        self._file_ext = scenario_format.file_ext()

    def expand(self, scenario: ScenarioModel) -> ScenarioModel:
        """
        Replace each ``RecipeStepModel`` in ``scenario`` with substituted recipe steps.
        """
        new_steps: list[StepModel | ParallelStepModel] = []
        for step in scenario.scenario:
            if isinstance(step, RecipeStepModel):
                new_steps.extend(self._expand_recipe(step))
            else:
                new_steps.append(step)
        scenario.scenario = new_steps
        return scenario

    def _expand_recipe(self, directive: RecipeStepModel) -> list[StepModel]:
        """
        Load and substitute a single recipe, returning ready-to-splice steps.
        """
        if not self._recipe_dirs:
            raise CliboaRuntimeError(
                f"'recipe: {directive.recipe}' was used but RECIPE_DIRS is "
                f"empty or undefined. Configure RECIPE_DIRS in your environment "
                f"to enable the recipe feature."
            )

        try:
            recipe_path = self._resolve_recipe_path(directive.recipe)
            self._logger.info(f"Resolved 'recipe: {directive.recipe}' to '{recipe_path}'")
            loader = self._loader_cls(recipe_path, True)
            recipe_model = RecipeModel.model_validate(loader())
            args_values = self._resolve_arguments(recipe_model, directive.arguments)
            recipe_model.apply_args(args_values)
        # Pass ValidationError through raw to match the main scenario
        # validation (we'd like to unify both later).
        except ValidationError:
            raise
        except Exception as e:
            raise ScenarioFileInvalid(f"'recipe: {directive.recipe}': {e}") from e

        return recipe_model.recipe

    def _resolve_recipe_path(self, raw_recipe_value: str) -> str:
        """
        Normalize ``raw_recipe_value`` and locate the recipe file under ``RECIPE_DIRS``.
        """
        stripped = raw_recipe_value.strip().lstrip("/")
        if not stripped:
            raise InvalidParameter("'recipe:' path is empty after stripping.")
        if any(c == ".." for c in stripped.split("/")):
            raise InvalidParameter(f"recipe path '{raw_recipe_value}' must not contain '..'.")

        candidates: list[str] = []
        for recipe_dir in self._recipe_dirs:
            candidate = path.join(recipe_dir, stripped + self._file_ext)
            candidates.append(candidate)
            if path.isfile(candidate):
                return candidate
        raise InvalidParameter(
            f"recipe '{stripped}' (with extension '{self._file_ext}') not found "
            f"in RECIPE_DIRS. Searched: {candidates}."
        )

    def _resolve_arguments(
        self,
        recipe_model: RecipeModel,
        passed: dict[str, str],
    ) -> dict[str, str]:
        """
        Build the ``args`` namespace by reconciling caller values with declared parameters.
        """
        declared = recipe_model.parameters

        for name in passed:
            if name not in declared:
                raise InvalidParameter(
                    f"extra argument '{name}' is not a declared parameter "
                    f"(declared: {sorted(declared.keys())})."
                )

        resolved: dict[str, str] = {}
        for name, spec in declared.items():
            if name in passed:
                resolved[name] = passed[name]
            elif spec.default is not None:
                resolved[name] = spec.default
            else:
                raise InvalidParameter(f"required parameter '{name}' was not provided.")
        return resolved
