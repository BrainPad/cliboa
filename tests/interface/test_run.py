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
import sys

from cliboa.interface import _parse_args


class TestCommandArgumentParser:
    def test_yaml_parse(self, monkeypatch):
        test_args = ["", "spam"]
        monkeypatch.setattr(sys, "argv", test_args)

        cmd_args = _parse_args()
        assert cmd_args.project_name == "spam"

    def test_json_parse(self, monkeypatch):
        test_args = ["project_name", "spam", "--format", "json"]
        monkeypatch.setattr(sys, "argv", test_args)

        cmd_args = _parse_args()
        assert cmd_args.project_name == "spam"
        assert cmd_args.format == "json"
