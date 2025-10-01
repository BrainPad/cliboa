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

from mock import call, patch

from cliboa.adapter.gcp import BigQueryAdapter, GcsAdapter
from cliboa.scenario.extract.gcp import BigQueryRead, GcsFileExistsCheck
from cliboa.util.helper import Helper
from cliboa.util.lisboa_log import LisboaLog
from tests import BaseCliboaTest


class TestBigQueryRead(BaseCliboaTest):
    @patch.object(BigQueryAdapter, "get_client")
    def test_execute_with_only_key(self, m_get_client):
        instance = BigQueryRead()
        Helper.set_property(instance, "project_id", "test_dataset")
        Helper.set_property(instance, "credentials", "test_tblname")
        Helper.set_property(instance, "dataset", "test_dataset")
        Helper.set_property(instance, "tblname", "test_tblname")
        Helper.set_property(instance, "location", "location")
        Helper.set_property(instance, "key", "test_key")
        Helper.set_property(instance, "query", "select * from *")
        Helper.set_property(instance, "dest_dir", "test_Dir")
        Helper.set_property(instance, "filename", "test_filename")
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        instance.execute()
        assert m_get_client.call_args_list == [
            call(credentials="test_tblname", project="test_dataset", location="location")
        ]

    @patch.object(BigQueryAdapter, "get_client")
    @patch.object(GcsAdapter, "get_client")
    @patch.object(BigQueryAdapter, "get_extract_job_config")
    @patch.object(BigQueryAdapter, "get_compression_type")
    @patch.object(BigQueryAdapter, "get_query_job_config")
    @patch.object(BigQueryAdapter, "get_write_disposition")
    def test_execute_with_only_bucket(
        self,
        m_bq_client,
        m_gcs_client,
        m_get_extract_job_config,
        m_get_compression_type,
        m_get_query_job_config,
        m_get_write_disposition,
    ):
        instance = BigQueryRead()
        Helper.set_property(instance, "project_id", "test_dataset")
        Helper.set_property(instance, "credentials", "test_tblname")
        Helper.set_property(instance, "dataset", "test_dataset")
        Helper.set_property(instance, "tblname", "test_tblname")
        Helper.set_property(instance, "location", "location")
        Helper.set_property(instance, "query", "select * from *")
        Helper.set_property(instance, "bucket", "test_bucket")
        Helper.set_property(instance, "dest_dir", "test_Dir")
        Helper.set_property(instance, "filename", "test_filename.csv")
        Helper.set_property(instance, "logger", LisboaLog.get_logger(__name__))
        instance.execute()
        assert m_bq_client.call_args_list == [call()]
        assert m_gcs_client.call_args_list == [call()]


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
