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
import tempfile

from mock import patch

from cliboa.adapter.aws import S3Adapter
from cliboa.scenario.extract.aws import S3Download, S3FileExistsCheck
from cliboa.test import BaseCliboaTest
from cliboa.util.helper import Helper

from cliboa.util.lisboa_log import LisboaLog


class TestS3Download(BaseCliboaTest):
    @patch.object(S3Adapter, "get_client")
    def test_execute_ok(self, m_get_client):
        with tempfile.TemporaryDirectory() as tmp_dir:
            m_get_object = m_get_client.return_value.get_object
            m_pagenate = m_get_client.return_value.get_paginator.return_value.paginate
            m_contents = [{"Contents": [{"Key": "spam"}]}]
            m_pagenate.return_value = m_contents

            instance = S3Download()
            Helper.set_property(instance, "bucket", "spam")
            Helper.set_property(instance, "src_pattern", "spam")
            Helper.set_property(instance, "dest_dir", tmp_dir)
            instance.execute()

            assert m_get_object.call_args_list == []


class TestS3FileExistsCheck(BaseCliboaTest):
    @patch.object(S3Adapter, "get_client")
    def test_execute_file_exists(self, m_get_client):
        m_get_object = m_get_client.return_value.get_object
        m_pagenate = m_get_client.return_value.get_paginator.return_value.paginate
        m_pagenate.return_value = [{"Contents": [{"Key": "spam"}]}]
        # テスト処理
        instance = S3FileExistsCheck()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "bucket", "spam")
        Helper.set_property(instance, "src_pattern", "spam")
        instance.execute()
        # 処理の正常終了を確認
        assert m_get_object.call_args_list == []

    @patch.object(S3Adapter, "get_client")
    def test_execute_file_not_exists(self, m_get_client):
        m_get_object = m_get_client.return_value.get_object
        m_pagenate = m_get_client.return_value.get_paginator.return_value.paginate
        m_pagenate.return_value = [{"Contents": [{"Key": "spam"}]}]
        # テスト処理
        instance = S3FileExistsCheck()
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        Helper.set_property(instance, "bucket", "spam")
        Helper.set_property(instance, "src_pattern", "hoge")
        instance.execute()
        # 処理の正常終了を確認
        assert m_get_object.call_args_list == []
