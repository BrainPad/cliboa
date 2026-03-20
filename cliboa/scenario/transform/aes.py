import os

from cryptography.fernet import Fernet

from cliboa.adapter.file import File
from cliboa.scenario.transform.file import FileBaseTransform


class AesBase(FileBaseTransform):
    class Arguments(FileBaseTransform.Arguments):
        key_dir: str
        key_pattern: str

    def execute(self, *args):
        files = self.get_src_files()
        if not self.check_file_existence(files):
            self.logger.info("No files are found. Nothing to do.")
            return

        key_file = self._resolve("adapter_file", File).get_target_files(
            self.args.key_dir, self.args.key_pattern
        )
        if len(key_file) != 1:
            self.logger.info("Key file must be only one.")
            return

        with open("".join(key_file), "r") as k:
            key = k.read()

        for file in files:
            dest_path = os.path.join(self.args.resolve_dest_dir(), os.path.basename(file))
            self._process(file, dest_path, key)

    def _process(self, file, dest_path, key) -> None:
        raise NotImplementedError()


class AesEncrypt(AesBase):
    def _process(self, file, dest_path, key) -> None:
        with open(file, "r") as f:
            data = f.read()

        encrypt_data = Fernet(key.encode()).encrypt(data.encode())
        with open(dest_path, "wb") as w:
            w.write(encrypt_data)


class AesDecrypt(AesBase):
    def _process(self, file, dest_path, key) -> None:
        with open(file, "rb") as f:
            data = f.read()

        decrypt_data = Fernet(key.encode()).decrypt(data)

        with open(dest_path, "wb") as w:
            w.write(decrypt_data)
