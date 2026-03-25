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
import os
import tempfile
from unittest.mock import MagicMock, call, patch

import gspread
import pytest

from cliboa.adapter.gcp import BigQueryAdapter
from cliboa.scenario.load.gcp import BigQueryCopy, GoogleSheetImport


def _gspread_api_error(status_code: int) -> gspread.exceptions.APIError:
    m_resp = MagicMock()
    m_resp.status_code = status_code
    m_resp.text = "err"
    m_resp.json.return_value = {
        "error": {
            "code": status_code,
            "message": "api error",
            "status": "ERROR",
        }
    }
    return gspread.exceptions.APIError(m_resp)


class TestBigQueryCopy:
    @patch.object(BigQueryAdapter, "get_client")
    def test_table_copy(self, m_get_bigquery_client):
        # Arrange
        gbq_client = m_get_bigquery_client.return_value

        instance = BigQueryCopy()
        instance._set_arguments(
            {
                "project_id": "awesome-project",
                "location": "asia-northeast1",
                "credentials": "/awesome-path/key.json",
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


class TestGoogleSheetImport:
    def _base_args(self, src_dir: str, src_pattern: str) -> dict:
        return {
            "project_id": "test-project",
            "credentials": "/path/to/key.json",
            "src_dir": src_dir,
            "src_pattern": src_pattern,
            "book_id": "spreadsheet-id-1",
            "sheet_name": "Data",
        }

    def test_execute_returns_1_when_zero_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            instance = GoogleSheetImport()
            instance._set_arguments(self._base_args(tmp, r".*\.csv"))
            ret = instance.execute()
        assert ret == 1

    def test_execute_returns_1_when_multiple_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            for name in ("a.csv", "b.csv"):
                path = os.path.join(tmp, name)
                with open(path, "w", encoding="utf-8") as f:
                    f.write("x\n1\n")
            instance = GoogleSheetImport()
            instance._set_arguments(self._base_args(tmp, r".*\.csv"))
            ret = instance.execute()
        assert ret == 1

    @patch("cliboa.scenario.load.gcp.gspread.authorize")
    @patch("cliboa.scenario.load.gcp.Credentials.from_service_account_file")
    def test_execute_clears_and_updates_existing_worksheet(self, m_from_sa_file, m_authorize):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "data.csv")
            with open(csv_path, "w", encoding="utf-8") as f:
                f.write("name,score\nAlice,10\nBob,\n")

            m_worksheet = MagicMock()
            m_sh = MagicMock()
            m_sh.worksheet.return_value = m_worksheet
            m_client = MagicMock()
            m_client.open_by_key.return_value = m_sh
            m_authorize.return_value = m_client

            instance = GoogleSheetImport()
            instance._set_arguments(self._base_args(tmp, r"data\.csv"))
            instance.execute()

        m_from_sa_file.assert_called_once()
        call_kw = m_from_sa_file.call_args
        assert call_kw[0][0] == "/path/to/key.json"
        assert set(call_kw[1]["scopes"]) == {
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        }
        m_authorize.assert_called_once_with(m_from_sa_file.return_value)
        m_client.open_by_key.assert_called_once_with("spreadsheet-id-1")
        m_sh.worksheet.assert_called_once_with("Data")
        m_worksheet.clear.assert_called_once()
        m_worksheet.update.assert_called_once()
        update_args, update_kwargs = m_worksheet.update.call_args
        assert update_kwargs == {"value_input_option": "USER_ENTERED"}
        payload = update_args[0]
        assert payload[0] == ["name", "score"]
        assert payload[1][0] == "Alice"
        assert payload[1][1] in (10, 10.0)
        assert payload[2] == ["Bob", ""]
        m_sh.add_worksheet.assert_not_called()

    @patch("cliboa.scenario.load.gcp.gspread.authorize")
    @patch("cliboa.scenario.load.gcp.Credentials.from_service_account_file")
    def test_execute_creates_worksheet_when_missing(self, m_from_sa_file, m_authorize):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "one.csv")
            with open(csv_path, "w", encoding="utf-8") as f:
                f.write("c1,c2\nv1,v2\n")

            m_worksheet = MagicMock()
            m_sh = MagicMock()
            m_sh.worksheet.side_effect = gspread.exceptions.WorksheetNotFound()
            m_sh.add_worksheet.return_value = m_worksheet
            m_client = MagicMock()
            m_client.open_by_key.return_value = m_sh
            m_authorize.return_value = m_client

            instance = GoogleSheetImport()
            instance._set_arguments(self._base_args(tmp, r"one\.csv"))
            instance.execute()

        m_sh.add_worksheet.assert_called_once()
        add_kw = m_sh.add_worksheet.call_args[1]
        assert add_kw["title"] == "Data"
        assert add_kw["rows"] >= 2
        assert add_kw["cols"] >= 2
        m_worksheet.clear.assert_not_called()
        m_worksheet.update.assert_called_once()
        update_args, _ = m_worksheet.update.call_args
        assert update_args[0] == [["c1", "c2"], ["v1", "v2"]]

    @patch("cliboa.scenario.load.gcp.time.sleep")
    @patch("cliboa.scenario.load.gcp.gspread.authorize")
    @patch("cliboa.scenario.load.gcp.Credentials.from_service_account_file")
    def test_execute_retries_transient_errors_until_success(
        self, m_from_sa_file, m_authorize, m_sleep
    ):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "data.csv")
            with open(csv_path, "w", encoding="utf-8") as f:
                f.write("a\n1\n")

            m_worksheet = MagicMock()
            m_sh = MagicMock()
            m_sh.worksheet.return_value = m_worksheet
            m_client = MagicMock()
            err = _gspread_api_error(503)
            m_client.open_by_key.side_effect = [err, err, m_sh]
            m_authorize.return_value = m_client

            instance = GoogleSheetImport()
            args = self._base_args(tmp, r"data\.csv")
            args["retry_count"] = 3
            args["retry_intvl_sec"] = 5
            instance._set_arguments(args)
            instance.execute()

        assert m_client.open_by_key.call_count == 3
        m_worksheet.update.assert_called_once()
        m_sleep.assert_has_calls([call(5), call(5)])

    @patch("cliboa.scenario.load.gcp.time.sleep")
    @patch("cliboa.scenario.load.gcp.gspread.authorize")
    @patch("cliboa.scenario.load.gcp.Credentials.from_service_account_file")
    def test_execute_retries_when_http_status_unavailable(
        self, m_from_sa_file, m_authorize, m_sleep
    ):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "data.csv")
            with open(csv_path, "w", encoding="utf-8") as f:
                f.write("a\n1\n")

            m_worksheet = MagicMock()
            m_sh = MagicMock()
            m_sh.worksheet.return_value = m_worksheet
            m_client = MagicMock()
            m_client.open_by_key.side_effect = [RuntimeError("no response"), m_sh]
            m_authorize.return_value = m_client

            instance = GoogleSheetImport()
            args = self._base_args(tmp, r"data\.csv")
            args["retry_intvl_sec"] = 3
            instance._set_arguments(args)
            instance.execute()

        assert m_client.open_by_key.call_count == 2
        m_sleep.assert_called_once_with(3)

    @patch("cliboa.scenario.load.gcp.time.sleep")
    @patch("cliboa.scenario.load.gcp.gspread.authorize")
    @patch("cliboa.scenario.load.gcp.Credentials.from_service_account_file")
    def test_execute_retries_on_http_429(self, m_from_sa_file, m_authorize, m_sleep):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "data.csv")
            with open(csv_path, "w", encoding="utf-8") as f:
                f.write("a\n1\n")

            m_worksheet = MagicMock()
            m_sh = MagicMock()
            m_sh.worksheet.return_value = m_worksheet
            m_client = MagicMock()
            m_client.open_by_key.side_effect = [_gspread_api_error(429), m_sh]
            m_authorize.return_value = m_client

            instance = GoogleSheetImport()
            args = self._base_args(tmp, r"data\.csv")
            args["retry_intvl_sec"] = 2
            instance._set_arguments(args)
            instance.execute()

        assert m_client.open_by_key.call_count == 2
        m_sleep.assert_called_once_with(2)

    @patch("cliboa.scenario.load.gcp.time.sleep")
    @patch("cliboa.scenario.load.gcp.gspread.authorize")
    @patch("cliboa.scenario.load.gcp.Credentials.from_service_account_file")
    def test_execute_stops_after_retry_count_failures(self, m_from_sa_file, m_authorize, m_sleep):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "data.csv")
            with open(csv_path, "w", encoding="utf-8") as f:
                f.write("a\n1\n")

            m_client = MagicMock()
            err = _gspread_api_error(503)
            m_client.open_by_key.side_effect = err
            m_authorize.return_value = m_client

            instance = GoogleSheetImport()
            args = self._base_args(tmp, r"data\.csv")
            args["retry_count"] = 3
            args["retry_intvl_sec"] = 4
            instance._set_arguments(args)
            with pytest.raises(gspread.exceptions.APIError):
                instance.execute()

        assert m_client.open_by_key.call_count == 3
        m_sleep.assert_has_calls([call(4), call(4)])

    @patch("cliboa.scenario.load.gcp.time.sleep")
    @patch("cliboa.scenario.load.gcp.gspread.authorize")
    @patch("cliboa.scenario.load.gcp.Credentials.from_service_account_file")
    def test_execute_does_not_sleep_when_no_retry(self, m_from_sa_file, m_authorize, m_sleep):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "data.csv")
            with open(csv_path, "w", encoding="utf-8") as f:
                f.write("a\n1\n")

            m_worksheet = MagicMock()
            m_sh = MagicMock()
            m_sh.worksheet.return_value = m_worksheet
            m_client = MagicMock()
            m_client.open_by_key.return_value = m_sh
            m_authorize.return_value = m_client

            instance = GoogleSheetImport()
            instance._set_arguments(self._base_args(tmp, r"data\.csv"))
            instance.execute()

        m_sleep.assert_not_called()

    @patch("cliboa.scenario.load.gcp.time.sleep")
    @patch("cliboa.scenario.load.gcp.gspread.authorize")
    @patch("cliboa.scenario.load.gcp.Credentials.from_service_account_file")
    def test_execute_does_not_retry_4xx_other_than_429(self, m_from_sa_file, m_authorize, m_sleep):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "data.csv")
            with open(csv_path, "w", encoding="utf-8") as f:
                f.write("a\n1\n")

            m_client = MagicMock()
            m_client.open_by_key.side_effect = _gspread_api_error(404)
            m_authorize.return_value = m_client

            instance = GoogleSheetImport()
            instance._set_arguments(self._base_args(tmp, r"data\.csv"))
            with pytest.raises(gspread.exceptions.APIError):
                instance.execute()

        assert m_client.open_by_key.call_count == 1
        m_sleep.assert_not_called()
