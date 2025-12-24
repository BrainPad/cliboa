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
import logging
from abc import abstractmethod
from typing import Any, List, Optional

from pydantic import BaseModel

from cliboa.adapter.file import File
from cliboa.scenario.interface import IParentStep
from cliboa.util.base import _BaseObject, _warn_deprecated


class AbstractStep(_BaseObject):
    """
    Abstract class of all the step classes.

    ALL step classes are required to inherit this class.
    This class is minimum implemented to be needed by core layer.
    """

    Arguments: type[BaseModel] | None = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._parent = None
        self._args = None

    @property
    def logger(self) -> logging.Logger:
        """
        logger instance
        """
        return self._logger

    @property
    def args(self) -> BaseModel | None:
        return self._args

    @property
    def parent(self) -> IParentStep | None:
        """
        parent instance which implements cliboa.scenario.interface.IParentStep
        """
        return self._parent

    @parent.setter
    def parent(self, parent: IParentStep):
        """
        set parent instance - assumed to set by cliboa
        """
        self._parent = parent

    def _set_arguments(self, arguments: dict[str, Any]) -> None:
        """
        This method allows you to set a value
        to the class with either method directly or via property setter.
        Either way, the method must be implemented
        to set the value for the class parameter like below.

        -- eg1 (recommended) --
        from pydantic import BaseModel
        class Foo(BaseStep):
            # no need to define __init__

            class Arguments(BaseModel):
                bar: string

            def execute(self):
                # access via self.args
                self.logger.info(f"bar is {self.args.bar}")

        -- eg2 --
        class Foo2(BaseStep):
            def __init__(self):
                self._bar = None

            def bar(self, bar):
                self._bar = bar

        -- eg3 --
        class Foo3(BaseStep):
            def __init__(self):
                self._bar = None

            @property
            def bar(self):
                return self._bar

            @bar.setter
            def bar(self, bar):
                self._bar = bar
        """
        props = arguments.copy()
        if self.Arguments is not None:
            self.logger.debug(f"Set arguments using {self.__class__.__name__}.Arguments")
            self._args = self.Arguments.model_validate(props)
            for field_name, field_info in self.Arguments.model_fields.items():
                if isinstance(field_info.validation_alias, str):
                    props.pop(field_info.validation_alias, None)
                elif field_info.alias:
                    props.pop(field_info.alias, None)
                else:
                    props.pop(field_name, None)
            if len(props) > 0:
                self.logger.warning(
                    f"There are {len(props)} unused arguments after assigned to pydantic model."
                )

        self.logger.debug("Set arguments to property directly.")
        for k, v in props.items():
            if isinstance(getattr(type(self), k, None), property):
                setattr(self, k, v)
            else:
                call = getattr(self, k, None)
                if callable(call):
                    call(v)
                else:
                    if self.Arguments is not None:
                        self.logger.warning(f"Failed to set mixed argument {k}")
                    else:
                        self.logger.warning(f"Failed to set argument {k}")

    @abstractmethod
    def execute(self, *args, **kwargs) -> Optional[int]:
        """
        Main logic of step
        """
        pass

    def get_symbol_argument(self, name: str) -> Any | None:
        """
        Returns a symbol's argument (variables are already transformed).
        Returns None if the argument cannot be retrieved, and no exceptions are raised.
        """
        if not self.parent:
            return None
        sa = self.parent.get_symbol_arguments()
        return sa.get(name)

    def put_to_context(self, value: Any) -> None:
        """
        Put any given value to cliboa's context.
        The value is stored using the current step's name as the key.
        """
        if not self.parent:
            return
        self.parent.put_to_context(value)

    def get_from_context(self, target: str | None = None) -> Any | None:
        """
        Get a value from cliboa's context.
        This value is typically placed there by preceding step (the target).
        If no target is specified, the method get the value associated with the step's symbol.
        Returns None if the value is not found (not raise an exception).
        """
        if not self.parent:
            return None
        return self.parent.get_from_context(target)


class BaseStep(AbstractStep):
    """
    Base class of all the step classes.

    This class has additional implement from AbstractStep to be useful on common cases.
    """

    @property
    def _step(self) -> str:
        """
        Deprecated: Kept for v2 backward compatibility.
        """
        self.logger.warning(
            _warn_deprecated(
                "cliboa.scenario.base.BaseStep._step",
                "3.0",
                "4.0",
                "cliboa.scenario.base.AbstractStep.put_to_context",
            )
        )
        return self.parent.step_name if self.parent else ""

    @property
    def _symbol(self) -> str | None:
        """
        Deprecated: Kept for v2 backward compatibility.
        """
        self.logger.warning(
            _warn_deprecated(
                "cliboa.scenario.base.BaseStep._symbol",
                "3.0",
                "4.0",
                "cliboa.scenario.base.AbstractStep.get_from_context",
            )
        )
        return self.parent.symbol_name if self.parent else None

    def get_step_argument(self, name: str) -> Any | None:
        """
        Deprecated: kept for v2 backward compatibility.
        """
        self.logger.warning(
            _warn_deprecated(
                "cliboa.scenario.base.BaseStep.get_step_argument",
                "3.0",
                "4.0",
                "cliboa.scenario.base.AbstractStep.get_symbol_argument",
            )
        )
        return self.get_symbol_argument(name)

    def _warn_deprecated_args(
        self, module_name: str, class_name: str, end_version: str, removal_version: str
    ) -> None:
        _warn_deprecated(
            f"directly arguments of {module_name}.{class_name}",
            end_version,
            removal_version,
            f"definition of {module_name}.{class_name}.Arguments",
        )

    def get_target_files(self, src_dir: str, src_pattern: str, *args, **kwargs) -> List[str]:
        """
        Alias of cliboa.adapter.File.get_target_files
        """
        return self._resolve("adaptor_file", File).get_target_files(
            src_dir, src_pattern, *args, **kwargs
        )
