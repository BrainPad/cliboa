from cliboa.adapter.mysql import MysqlAdaptor
from contextlib import ExitStack
from tests import BaseCliboaTest
from unittest.mock import patch, Mock, MagicMock


class TestRdbms(BaseCliboaTest):
    """
    Transaction commit.
    """
    def test_mysql_commit(self):
        with ExitStack() as stack:
            stack.enter_context(patch('cliboa.adapter.mysql.pymysql'))
            with MysqlAdaptor(host="dummmy",
                              user="test",
                              password="password",
                              dbname="test"):
                pass

    def test_mysql_rollback(self):
        """
        Transaction rollback.
        """
        with ExitStack() as stack:
            stack.enter_context(patch('cliboa.adapter.mysql.pymysql'))
            try:
                with MysqlAdaptor(host="dummmy",
                                  user="test",
                                  password="password",
                                  dbname="test"):
                    raise Exception('Error occurred')
            except Exception:
                pass

    def _create_dbmock(self, mock_obj):
        mock_con = Mock()
        mock_obj.connect.return_value = mock_con

        mock_cursor = MagicMock()
        mock_result = MagicMock()

        mock_cursor.__enter__.return_value = mock_result
        mock_con.cursor.return_value = mock_cursor
