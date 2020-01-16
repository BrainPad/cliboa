import logging


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
        return logging.getLogger(modname)
