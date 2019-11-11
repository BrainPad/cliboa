import logging.config

import cliboa
from cliboa.conf import env


class LisboaLog(object):
    """
    Logger class for lisboa
    """

    @staticmethod
    def get_logger(modname):
        """
        Get logger

        Args:
            modname (str): module name
        Returns:
            logger instance
        """
        # load logging.conf
        logging.config.fileConfig(
            env.BASE_DIR + "/conf/logging.conf", disable_existing_loggers=False
        )
        return logging.getLogger(modname)
