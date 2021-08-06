from contextlib import ExitStack
from unittest.mock import MagicMock, Mock, patch

from cliboa.adapter.mysql import MysqlAdaptor
from cliboa.test import BaseCliboaTest


class TestRdbms(BaseCliboaTest):
    """
    Transaction commit.
    """

    def test_mysql_commit(self):
        with ExitStack() as stack:
            stack.enter_context(patch("cliboa.adapter.mysql.pymysql"))
            with MysqlAdaptor(
                host="dummy", user="test", password="dummypassword", dbname="test"
            ):
                pass

    def test_mysql_rollback(self):
        """
        Transaction rollback.
        """
        with ExitStack() as stack:
            stack.enter_context(patch("cliboa.adapter.mysql.pymysql"))
            try:
                with MysqlAdaptor(
                    host="dummy", user="test", password="dummypassword", dbname="test"
                ):
                    raise Exception("Error occurred")
            except Exception:
                pass

    def _create_dbmock(self, mock_obj):
        mock_con = Mock()
        mock_obj.connect.return_value = mock_con

        mock_cursor = MagicMock()
        mock_result = MagicMock()

        mock_cursor.__enter__.return_value = mock_result
        mock_con.cursor.return_value = mock_cursor
