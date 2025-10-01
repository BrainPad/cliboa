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

from cliboa.util.date import DateUtil


class TestDateUtil(object):
    def test_get_logger(self):

        res = DateUtil().convert_date_format("2019/1/01 00:00:00", "%Y-%m-%d %H:%M:%S")
        assert res == "2019-01-01 00:00:00"

        res = DateUtil().convert_date_format("2019/1/01 00:00:00", "%Y-%m-%d %H:00")
        assert res == "2019-01-01 00:00"

        res = DateUtil().convert_date_format("2019-01-01 00:00:00", "%Y%m%d%H%M%S")
        assert res == "20190101000000"
