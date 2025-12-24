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
from cliboa.util.base import _warn_deprecated

global _PROCESS_STORE_CACHE
_PROCESS_STORE_CACHE = {}


class ObjectStore:
    """
    Cache any object.
    This cache class is used when same parameter uses from one STEP to the other.
    """

    @staticmethod
    def put(k, v, quiet: bool = False):
        """
        Put value

        Args:
            k (str): Cache key
            v (dict): Cache value
        """
        if not quiet:
            _warn_deprecated("cliboa.util.cache.ObjectStore.put", "3.0", "4.0")
        _PROCESS_STORE_CACHE[k] = v

    @staticmethod
    def get(k):
        """
        Get value

        Args:
            k (str): Cache key

        Returns:
            dict: Value. Returns None if the key does not exist
        """
        _warn_deprecated("cliboa.util.cache.ObjectStore.get", "3.0", "4.0")
        return _PROCESS_STORE_CACHE.get(k)
