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
import pytest

from cliboa.scenario.validator import EssentialParameters
from cliboa.util.exception import CliboaException


class TestEssentialParameters(object):
    def test_essential_parameters_ng(self):
        """
        EssentialParameters invalid case
        """
        with pytest.raises(CliboaException) as excinfo:
            valid = EssentialParameters("DummyClass", [""])
            valid()
        assert "is not specified" in str(excinfo.value)
