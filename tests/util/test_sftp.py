import sys
from contextlib import ExitStack
from cliboa.util.sftp import Sftp
from unittest.mock import patch
from tests import BaseCliboaTest


class TestSftp(BaseCliboaTest):
    def test_list_files(self):
        if sys.version_info.minor < 6:
            # ignore test if python version is less 3.6(assert_called is not supported)
            return

        with ExitStack() as stack:
            mock_func = stack.enter_context(patch('cliboa.util.sftp.list_file_func'))
            mock_func.return_value = ["test.txt"]
            stack.enter_context(patch('paramiko.SSHClient.connect'))
            stack.enter_context(patch('paramiko.SSHClient.open_sftp'))
            sftp = Sftp(host="test", user="admin", password="pass")
            sftp.list_files(None, None, None)
            mock_func.assert_called_once()

    def test_clear_files(self):
        if sys.version_info.minor < 6:
            # ignore test if python version is less 3.6(assert_called is not supported)
            return

        with ExitStack() as stack:
            mock_func = stack.enter_context(patch('cliboa.util.sftp.clear_file_func'))
            stack.enter_context(patch('paramiko.SSHClient.connect'))
            stack.enter_context(patch('paramiko.SSHClient.open_sftp'))
            sftp = Sftp(host="test", user="admin", password="pass")
            sftp.clear_files(None, None)
            mock_func.assert_called_once()

    def test_remove_specific_file(self):
        if sys.version_info.minor < 6:
            # ignore test if python version is less 3.6(assert_called is not supported)
            return

        with ExitStack() as stack:
            mock_func = stack.enter_context(patch('cliboa.util.sftp.remove_specific_file_func'))
            stack.enter_context(patch('paramiko.SSHClient.connect'))
            stack.enter_context(patch('paramiko.SSHClient.open_sftp'))
            sftp = Sftp(host="test", user="admin", password="pass")
            sftp.remove_specific_file(None, None)
            mock_func.assert_called_once()

    def test_get_specific_file(self):
        if sys.version_info.minor < 6:
            # ignore test if python version is less 3.6(assert_called is not supported)
            return

        with ExitStack() as stack:
            mock_func = stack.enter_context(patch('cliboa.util.sftp.get_specific_file_func'))
            stack.enter_context(patch('paramiko.SSHClient.connect'))
            stack.enter_context(patch('paramiko.SSHClient.open_sftp'))
            sftp = Sftp(host="test", user="admin", password="pass")
            sftp.get_specific_file(None, None)
            mock_func.assert_called_once()

    def test_put_file(self):
        if sys.version_info.minor < 6:
            # ignore test if python version is less 3.6(assert_called is not supported)
            return

        with ExitStack() as stack:
            mock_func = stack.enter_context(patch('cliboa.util.sftp.put_file_func'))
            stack.enter_context(patch('paramiko.SSHClient.connect'))
            stack.enter_context(patch('paramiko.SSHClient.open_sftp'))
            sftp = Sftp(host="test", user="admin", password="pass")
            sftp.put_file(None, None)
            mock_func.assert_called_once()
