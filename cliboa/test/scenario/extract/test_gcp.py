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
from cliboa.scenario.extract.gcp import GcsToBigQuery
from cliboa.util.helper import Helper
from unittest.mock import patch
from cliboa.util.gcp import BigQuery, Gcs


class TestBigQueryCopy(object):

    @patch.object(GcsToBigQuery, "_source_path_reader", return_value="/awesome-path/key.json")
    
    @patch.object(BigQuery, "get_bigquery_client")
    def test_table_copy(self, m_get_bigquery_client, mock_path_reader):
        # Arrange
        gbq_client = m_get_bigquery_client.return_value

        instance = BigQueryCopy()
        Helper.set_property(instance, "project_id", "awesome-project")
        Helper.set_property(instance, "location", "asia-northeast1")
        Helper.set_property(instance, "credentials", {"file": "/awesome-path/key.json"})
        Helper.set_property(instance, "dataset", "awesome_dataset")
        Helper.set_property(instance, "tblname", "awesome_table")
        Helper.set_property(instance, "dest_dataset", "copy_awesome_dataset")
        Helper.set_property(instance, "dest_tblname", "copy_awesome_table")
        instance.execute()

        # Tests
        m_get_bigquery_client.assert_called_with(
            credentials='/awesome-path/key.json',
            location='asia-northeast1',
            project='awesome-project'
        )

        gbq_client.copy_table.assert_called_with(
            'awesome-project.awesome_dataset.awesome_table',
            'awesome-project.copy_awesome_dataset.copy_awesome_table'
        )
