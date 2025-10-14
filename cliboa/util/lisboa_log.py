"""
Deprecated
TODO: remove this module until v3.0 stable release
"""

from cliboa.util.base import _warn_deprecated
from cliboa.util.log import _get_logger


class LisboaLog(object):
    @staticmethod
    def get_logger(modname):
        _warn_deprecated("cliboa.util.lisboa_log.LisboaLog")
        return _get_logger(modname)
