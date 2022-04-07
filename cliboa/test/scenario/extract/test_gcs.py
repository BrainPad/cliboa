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

from cliboa.adapter.gcp import GcsAdapter
from cliboa.scenario.extract.gcp import GcsFileExistsCheck
from cliboa.test import BaseCliboaTest
from cliboa.util.helper import Helper
from mock import patch

from cliboa.util.lisboa_log import LisboaLog


class TestGcsFileExistsCheck(BaseCliboaTest):
    @patch.object(GcsAdapter, "get_client")
    def test_execute_file_exists_ok(self, m_get_client):
        m_get_object = m_get_client.return_value.get_object
        m_pagenate = m_get_client.return_value.get_paginator.return_value.paginate
        m_contents = [{"Contents": [{"Key": "spam"}]}]
        m_pagenate.return_value = m_contents
        # テスト処理
        instance = GcsFileExistsCheck()
        Helper.set_property(instance, "project_id", "hoge")
        Helper.set_property(instance, "bucket", "piyo")
        Helper.set_property(instance, "src_pattern", "spam")
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        instance.execute()
        # 処理の正常終了を確認
        assert m_get_object.call_args_list == []

    @patch.object(GcsAdapter, "get_client")
    def test_execute_file_not_exists_ok(self, m_get_client):
        m_get_object = m_get_client.return_value.get_object
        m_pagenate = m_get_client.return_value.get_paginator.return_value.paginate
        m_contents = [{"Contents": [{"Key": "spam"}]}]
        m_pagenate.return_value = m_contents
        # テスト処理
        instance = GcsFileExistsCheck()
        Helper.set_property(instance, "project_id", "hoge")
        Helper.set_property(instance, "bucket", "piyo")
        Helper.set_property(instance, "src_pattern", "spam1")
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        instance.execute()
        # 処理の正常終了を確認
        assert m_get_object.call_args_list == []
