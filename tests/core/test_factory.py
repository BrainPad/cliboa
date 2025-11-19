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

from cliboa.core.factory import _CliboaFactory
from cliboa.scenario import ExecuteShellScript
from cliboa.util.exception import InvalidScenarioClass


class TestCliboaFactory:
    def test_create_ok(self):
        instance = _CliboaFactory().create("ExecuteShellScript")
        assert type(instance) is ExecuteShellScript

    def test_create_ng(self):
        with pytest.raises(InvalidScenarioClass):
            _CliboaFactory().create("NotFoundClass")

    @pytest.mark.skip(
        "The factory is scheduled for a redesign for v3"
        " which will eliminate the sys.path.append dependency."
    )
    def test_execute_with_candidates(self):
        # sys.path.append("cliboa/scenario")
        custom_instance = _CliboaFactory()._create_custom_instance("SampleStep")
        assert custom_instance is not None
