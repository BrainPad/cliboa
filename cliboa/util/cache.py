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
global _STEP_ARGUMENT_CACHE
global _PROCESS_STORE_CACHE
_STEP_ARGUMENT_CACHE = {}
_PROCESS_STORE_CACHE = {}


class StepArgument(object):
    """
    Cache arguments defined in yaml with step name.
    """

    @staticmethod
    def _put(step, instance):
        """
        Store step arguments

        Args:
            step (str): Cache key (step name)
            instance (class): Step class
        """
        props = instance.__dict__
        items = {}
        for k, v in props.items():
            # By default, property keys are changed by python language specification.
            # Adjust key names to the scenario.yaml defined name
            # (both removed underscore and manglinged prefix)
            sp = "_%s_" % instance.__class__.__name__
            if k.startswith(sp):
                k = k.split(sp)[1]
            k = k[1:] if k.startswith("_") else k
            items[k] = v
        _STEP_ARGUMENT_CACHE[step] = items

    @staticmethod
    def get(k):
        """
        Get value

        Args:
            k (str): Cache key

        Returns:
            dict: Value (underscore removed). Returns None if the key does not exist
        """
        return _STEP_ARGUMENT_CACHE.get(k)


class ObjectStore(object):
    """
    Cache any object.
    This cache class is used when same parameter uses from one STEP to the other.
    """

    @staticmethod
    def put(k, v):
        """
        Put value

        Args:
            k (str): Cache key
            v (dict): Cache value
        """
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
        return _PROCESS_STORE_CACHE.get(k)
