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
from multiprocessing import Pool

import cloudpickle
from multiprocessing_logging import install_mp_handler

from cliboa import state
from cliboa.core.interface import _IExecute
from cliboa.core.model import ParallelConfigModel
from cliboa.util.base import _BaseObject
from cliboa.util.constant import StepStatus
from cliboa.util.exception import StepExecutionFailed
from cliboa.util.log import _get_logger


class _ParallelProcessor(_BaseObject, _IExecute):
    """
    Parallel processing decorator class for _StepExecutor instances.
    """

    def __init__(
        self, steps: list[_IExecute], config: ParallelConfigModel | None = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._steps = steps
        if not config:
            config = ParallelConfigModel()
        config.fill_default()
        self._config = config

    @staticmethod
    def _async_step_execute(cls):
        try:
            clz = cloudpickle.loads(cls)
            res = clz.execute()
            if res == StepStatus.ABNORMAL_TERMINATION:
                return "NG"
            else:
                return "OK"
        except Exception as e:
            _get_logger(__name__).error(e)
            return "NG"

    def execute(self) -> int | None:
        state.set("_ParallelExecute")
        self._logger.info(
            "Multi process start. Execute step count=%s." % self._config.multi_process_count
        )
        install_mp_handler()
        packed = [cloudpickle.dumps(step) for step in self._steps]

        try:
            with Pool(processes=self._config.multi_process_count) as p:
                for r in p.imap_unordered(self._async_step_execute, packed):
                    if r == "NG":
                        if self._config.force_continue:
                            self._logger.warning("Multi process response. %s" % r)
                        else:
                            raise StepExecutionFailed("Multi process response. %s" % r)
        except Exception:
            self._logger.exception("Exception occurred during multi process execution.")
            return StepStatus.ABNORMAL_TERMINATION
