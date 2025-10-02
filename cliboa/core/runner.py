import logging
from types import SimpleNamespace
from typing import List

from cliboa import state
from cliboa.conf import env
from cliboa.core.factory import ScenarioManagerFactory
from cliboa.core.listener import ScenarioStatusListener
from cliboa.core.worker import ScenarioWorker
from cliboa.util.lisboa_log import LisboaLog
from cliboa.util.log_record import CliboaLogRecord


class ScenarioRunner(object):
    """
    Scenario Runner
    """

    def __init__(
        self,
        project_name: str,
        scenario_format: str = "yaml",
        execute_method_argument: List[str] = [],
        load_logging_conf: bool = True,
        set_cliboa_log: bool = True,
    ):
        """
        Initialize and prepare cliboa execution environment.
        """
        if load_logging_conf:
            # Load {BASE_DIR}/conf/logging.conf
            logging.config.fileConfig(
                env.BASE_DIR + "/conf/logging.conf", disable_existing_loggers=False
            )
        if set_cliboa_log:
            logging.setLogRecordFactory(CliboaLogRecord)
        self._logger = LisboaLog.get_logger(__name__)

        # params
        self._project_name = project_name
        self._scenario_format = scenario_format
        self._execute_method_argument = execute_method_argument

    def execute(self):
        """
        Execute whole cliboa process.
        """
        self._create_scenario_queue()
        return self._execute_scenario()

    def _create_scenario_queue(self):
        """
        Create scenario queue
        """
        state.set("_LoadScenario")
        manager = ScenarioManagerFactory.create(self._project_name, self._scenario_format)
        manager.create_scenario_queue()

    def _execute_scenario(self):
        """
        Execute scenario
        """
        state.set("_WorkScenario")
        # TODO: remove cmd_args by worker
        cmd_args = {
            "project_name": self._project_name,
            "execute_method_argument": self._execute_method_argument,
            "format": self._scenario_format,
        }
        worker = ScenarioWorker(SimpleNamespace(**cmd_args))
        worker.register_listeners(ScenarioStatusListener())
        return worker.execute_scenario()
