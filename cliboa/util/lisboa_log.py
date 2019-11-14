import logging.config

import cliboa
from cliboa.conf import env
from multiprocessing_logging import install_mp_handler

class LisboaLog(object):
    """
    Logger class for lisboa
    """

    @staticmethod
    def get_logger(modname, is_multi_proc=False):
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
        logger = logging.getLogger(modname)
        if is_multi_proc is True:
            install_mp_handler(logger)
        return logger
