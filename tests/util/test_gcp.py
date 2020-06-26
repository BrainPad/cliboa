#
# Copyright 2020 BrainPad Inc. All Rights Reserved.
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
from unittest import TestCase

import pytest
from google.cloud import bigquery, storage
from google.oauth2 import service_account

from cliboa.util.gcp import BigQuery, Gcs, ServiceAccount


class TestServiceAccount(TestCase):
    @pytest.mark.skip(reason="service account connection is neccesary")
    def test_auth_no_credentials(self):
        assert ServiceAccount.auth(None) == ""


class TestBigQuery(TestCase):
    @pytest.mark.skip(reason="bigquery connection is neccesary")
    def test_get_bigquery_client_no_credentialas(self):
        assert BigQuery.get_bigquery_client(None) == bigquery.Client()

    def test_get_extract_job_config_with_header(self):
        self.assertTrue(
            isinstance(
                BigQuery.get_extract_job_config(), type(bigquery.ExtractJobConfig())
            )
        )

    def test_get_extract_job_config_with_no_header(self):
        self.assertTrue(
            isinstance(
                BigQuery.get_extract_job_config(print_header=False),
                type(bigquery.ExtractJobConfig(print_header=False)),
            ),
        )

    def test_get_query_job_config(self):
        self.assertTrue(
            isinstance(BigQuery.get_query_job_config(), type(bigquery.QueryJobConfig()))
        )

    def test_get_comporession_type(self):
        assert BigQuery.get_compression_type() == "GZIP"

    def test_get_destination_format_csv(self):
        assert BigQuery.get_destination_format(".csv") == "CSV"

    def test_get_destination_format_json(self):
        assert BigQuery.get_destination_format(".json") == "NEWLINE_DELIMITED_JSON"


class TestGcs(object):
    @pytest.mark.skip(reason="gcs connection is neccesary")
    def test_get_gcs_client_no_credentials(self):
        assert Gcs.get_gcs_client(None) == storage.Client()
