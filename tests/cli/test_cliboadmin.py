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
import os
import shutil
import sys

import pytest

from cliboa.cli.cliboadmin import CliboAdmin, CommandArgumentParser
from tests import BaseCliboaTest


class TestCommandArgumentParser(BaseCliboaTest):
    def setup_method(self, method):
        sys.argv.clear()
        sys.argv.append("")
        sys.argv.append("init")
        sys.argv.append("dummy_init_dir")

    def test_parse(self):
        cmd_parser = CommandArgumentParser()
        cmd_args = cmd_parser.parse()
        assert cmd_args.option == "init"


class TestCliboAdmin(BaseCliboaTest):
    @pytest.fixture(autouse=True)
    def setup_resource(self):
        if os.path.exists("dummy_init_dir"):
            shutil.rmtree("dummy_init_dir")
        if os.path.exists("project"):
            shutil.rmtree("project")
        yield "test in progress"
        if os.path.exists("dummy_init_dir"):
            shutil.rmtree("dummy_init_dir")
        if os.path.exists("project"):
            shutil.rmtree("project")

    def test_main_init(self):
        sys.argv.clear()
        sys.argv.append("")
        sys.argv.append("init")
        sys.argv.append("dummy_init_dir")
        parser = CommandArgumentParser()
        args = parser.parse()
        admin = CliboAdmin(args)
        admin.main()

    def test_main_create(self):
        sys.argv.clear()
        sys.argv.append("")
        sys.argv.append("create")
        sys.argv.append("dummy_project")
        parser = CommandArgumentParser()
        args = parser.parse()
        admin = CliboAdmin(args)
        admin.main()
