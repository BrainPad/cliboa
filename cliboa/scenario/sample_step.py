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
from pydantic import BaseModel

from cliboa.scenario.base import BaseStep


class SampleStep(BaseStep):
    """
    For unit test
    """

    class Arguments(BaseModel):
        retry_count: int = 3
        memo: str | None = None

    def execute(self):
        self.logger.info("Start %s" % self.__class__.__name__)
        self.logger.info(f"my memo is {self.args.memo}")
        self.logger.info("Finish %s" % self.__class__.__name__)


class SampleStepSub(SampleStep):
    """
    For unit test
    """

    class Arguments(SampleStep.Arguments):
        name: str | None = None

    def execute(self, **kwargs):
        self.logger.info(f"Start {self}")
        self.logger.info(f"kwargs is {kwargs}")
        self.logger.info(f"my name is {self.args.name}")
        self.logger.info(f"my memo is {self.args.memo}")
        symbol_memo = self.get_symbol_argument("memo")
        self.logger.info(f"symbol memo is {symbol_memo}")
        symbol_context = self.get_from_context()
        self.logger.info(f"symbol context is {symbol_context}")
        self.logger.info("Finish %s" % self.__class__.__name__)


class SampleCustomStep(BaseStep):
    """
    For unit test
    """

    class Arguments(BaseModel):
        password: str | None = None
        access_key: str | None = None
        secret_key: str | None = None
        access_token: str | None = None
        retry_count: int = 3

    def execute(self):
        pass
