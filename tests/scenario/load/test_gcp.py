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
from unittest.mock import patch

from cliboa.adapter.gcp import BigQueryAdapter
from cliboa.scenario.load.gcp import BigQueryCopy


class TestBigQueryCopy(object):
    @patch.object(BigQueryCopy, "_source_path_reader", return_value="/awesome-path/key.json")
    @patch.object(BigQueryAdapter, "get_client")
    def test_table_copy(self, m_get_bigquery_client, mock_path_reader):
        # Arrange
        gbq_client = m_get_bigquery_client.return_value

        instance = BigQueryCopy()
        instance._set_properties(
            {
                "project_id": "awesome-project",
                "location": "asia-northeast1",
                "credentials": {"file": "/awesome-path/key.json"},
                "dataset": "awesome_dataset",
                "tblname": "awesome_table",
                "dest_dataset": "copy_awesome_dataset",
                "dest_tblname": "copy_awesome_table",
            }
        )
        instance.execute()

        # Tests
        m_get_bigquery_client.assert_called_with(
            credentials="/awesome-path/key.json",
            location="asia-northeast1",
            project="awesome-project",
        )

        gbq_client.copy_table.assert_called_with(
            "awesome-project.awesome_dataset.awesome_table",
            "awesome-project.copy_awesome_dataset.copy_awesome_table",
        )
