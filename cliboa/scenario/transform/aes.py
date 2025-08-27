import os

from cryptography.fernet import Fernet

from cliboa.scenario.base import BaseStep
from cliboa.scenario.validator import EssentialParameters


class AesBase(BaseStep):
    def __init__(self):
        super().__init__()
        self._src_dir = None
        self._src_pattern = None
        self._dest_dir = None
        self._key_dir = None
        self._key_pattern = None

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def key_dir(self, key_dir):
        self._key_dir = key_dir

    def key_pattern(self, key_pattern):
        self._key_pattern = key_pattern

    def execute(self, *args):
        pass


class AesEncrypt(AesBase):
    def __init__(self):
        super().__init__()

    def execute(self, *args):
        valid = EssentialParameters(self.__class__.__name__, [self._src_dir, self._src_pattern])
        valid()

        if self._dest_dir:
            os.makedirs(self._dest_dir, exist_ok=True)

        files = super().get_target_files(self._src_dir, self._src_pattern)
        if len(files) == 0:
            self._logger.info("No files are found. Nothing to do.")
            return

        key_file = super().get_target_files(self._key_dir, self._key_pattern)
        if len(key_file) != 1:
            self._logger.info("Key file must be only one.")
            return

        with open("".join(key_file), "r") as k:
            key = k.read()

        for file in files:
            dest_path = (
                os.path.join(self._dest_dir, os.path.basename(file))
                if self._dest_dir is not None
                else os.path.join(self._src_dir, os.path.basename(file))
            )
            with open(file, "r") as f:
                data = f.read()

            encrypt_data = Fernet(key.encode()).encrypt(data.encode())
            with open(dest_path, "wb") as w:
                w.write(encrypt_data)


class AesDecrypt(AesBase):
    def __init__(self):
        super().__init__()

    def execute(self, *args):
        valid = EssentialParameters(self.__class__.__name__, [self._src_dir, self._src_pattern])
        valid()

        if self._dest_dir:
            os.makedirs(self._dest_dir, exist_ok=True)

        files = super().get_target_files(self._src_dir, self._src_pattern)
        if len(files) == 0:
            self._logger.info("No files are found. Nothing to do.")
            return

        key_file = super().get_target_files(self._key_dir, self._key_pattern)
        if len(key_file) != 1:
            self._logger.info("Key file must be only one.")
            return

        with open("".join(key_file), "r") as k:
            key = k.read()

        for file in files:
            dest_path = (
                os.path.join(self._dest_dir, os.path.basename(file))
                if self._dest_dir is not None
                else os.path.join(self._src_dir, os.path.basename(file))
            )
            with open(file, "rb") as f:
                data = f.read()

            decrypt_data = Fernet(key.encode()).decrypt(data)

            with open(dest_path, "wb") as w:
                w.write(decrypt_data)
