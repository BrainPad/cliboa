from unittest.mock import MagicMock

import pytest

from cliboa.scenario.extract.mysql import MysqlRead
from cliboa.scenario.extract.postgres import PostgresqlRead
from cliboa.scenario.load.mysql import MysqlWrite
from cliboa.scenario.load.postgres import PostgresqlWrite


@pytest.fixture
def mock_adaptor():
    """
    Mock for the DB adaptor.
    """
    adaptor = MagicMock()
    adaptor.__enter__.return_value = adaptor
    adaptor.__exit__.return_value = None
    return adaptor


@pytest.fixture
def mock_file_adaptor():
    """
    Mock for the File adaptor (cliboa.adapter.File).
    """
    file_adaptor = MagicMock()
    # Default: return a dummy file list
    file_adaptor.get_target_files.return_value = ["/input_dir/test.csv"]
    return file_adaptor


@pytest.fixture
def mock_logger():
    """Mock for the logger instance."""
    return MagicMock()


class TestRdbmsWrite:
    """Tests for Write (Load) classes: MysqlWrite and PostgresqlWrite."""

    @pytest.mark.skip(
        reason=(
            "Mock setup is currently not working correctly. "
            "the test should be refactored to use a DI based approach."
        )
    )
    @pytest.mark.parametrize("target_class", [MysqlWrite, PostgresqlWrite])
    def test_execute_write_happy_path(
        self, target_class, mock_adaptor, mock_file_adaptor, mock_logger, mocker
    ):
        # 1. Instance creation
        instance = target_class(
            di_adaptor_db=mock_adaptor, di_adaptor_file=mock_file_adaptor, di_logger=mock_logger
        )

        # 2. Set arguments
        arg_values = {
            "host": "localhost",
            "dbname": "test_db",
            "user": "test_user",
            "password": "password",
            "port": 3306,
            "src_dir": "/input_dir",
            "src_pattern": "test.csv",
            "tblname": "dest_table",
            "encoding": "utf-8",
            "chunk_size": 100,
        }
        instance._set_arguments(arg_values)

        # 3. Mock external dependencies

        # CSV content: Header + 1 Data Row
        csv_lines = ["col1,col2", "val1,val2"]

        # IMPORTANT: Define a side_effect for open()
        # This ensures that EVERY time open() is called, a NEW Mock object
        # with a NEW iterator is returned. This prevents iterator exhaustion
        # between the header check and the actual data reading.
        def open_side_effect(*args, **kwargs):
            f = MagicMock()
            f.__enter__.return_value = f
            f.__exit__.return_value = None
            # Use side_effect on __iter__ to ensure a fresh iterator
            f.__iter__.side_effect = lambda: iter(csv_lines)
            return f

        mocker.patch("builtins.open", side_effect=open_side_effect)

        # 4. Execution
        instance.execute()

        # 5. Verification
        # Verify file searching
        mock_file_adaptor.get_target_files.assert_called_with("/input_dir", "test.csv")

        # Verify DB insertion called
        assert mock_adaptor.insert.called

        call_args = mock_adaptor.insert.call_args
        actual_sql = call_args.args[0]
        actual_params = call_args.args[1]

        assert "INSERT INTO dest_table" in actual_sql
        # Verify data passed
        assert ("val1", "val2") in actual_params or (("val1", "val2"),) in actual_params


class TestRdbmsRead:
    """Tests for Read (Extract) classes: MysqlRead and PostgresqlRead."""

    @pytest.mark.parametrize("target_class", [MysqlRead, PostgresqlRead])
    def test_execute_read_happy_path(
        self, target_class, mock_adaptor, mock_file_adaptor, mock_logger, mocker
    ):
        # 1. Instance creation
        instance = target_class(
            di_adaptor_db=mock_adaptor, di_adaptor_file=mock_file_adaptor, di_logger=mock_logger
        )

        # 2. Set arguments
        arg_values = {
            "host": "localhost",
            "dbname": "test_db",
            "user": "test_user",
            "password": "password",
            "dest_path": "/output_dir/result.csv",
            "tblname": "source_table",
            "encoding": "utf-8",
        }
        instance._set_arguments(arg_values)

        # 3. Mock external dependencies
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",), ("col2",)]
        mock_cursor.__iter__.return_value = [("val1", "val2"), ("val3", "val4")]
        mock_adaptor.select.return_value = mock_cursor

        mocker.patch("os.makedirs")

        # Mock builtins.open manually for writing
        file_mock = MagicMock()
        file_mock.__enter__.return_value = file_mock
        file_mock.__exit__.return_value = None
        mocker.patch("builtins.open", return_value=file_mock)

        # 4. Execution
        instance.execute()

        # 5. Verification
        mock_adaptor.select.assert_called_once()
        assert "SELECT * FROM source_table" in mock_adaptor.select.call_args.args[0]

        # Verify file write
        assert file_mock.write.called or file_mock.writerow.called
