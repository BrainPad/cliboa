import gnupg
import logging
import os
from cliboa.util.exception import CliboaException


class Gpg(object):
    def __init__(self, gnupghome):
        super().__init__()

        if not os.path.exists(gnupghome):
            os.makedirs(gnupghome, exist_ok=True)

        self.log = logging.getLogger(__name__)
        self._gpg = gnupg.GPG(gnupghome=gnupghome)

    def generate_key(self, dest_dir, name_real=None, name_email=None, passphrase=None):
        input_data = self._gpg.gen_key_input(
            name_real=name_real,
            name_email=name_email,
            passphrase=passphrase,
        )

        key = self._gpg.gen_key(input_data)

        public_keys = self._gpg.export_keys(key.fingerprint)
        private_keys = self._gpg.export_keys(
            keyids=key.fingerprint,
            secret=True,
            passphrase=passphrase,
        )

        with open(os.path.join(dest_dir, "public"), "w") as f:
            f.write(public_keys)

        with open(os.path.join(dest_dir, "private"), "w") as f:
            f.write(private_keys)

    def import_key(self, key_path):
        with open(key_path) as f:
            key_data = f.read()
            import_result = self._gpg.import_keys(key_data)

        self.log.info("Import key count: %s " % import_result.count)

    def trust_key(self, key_path, trust_level):
        keys = self._gpg.scan_keys(key_path)
        for key in keys:
            self._gpg.trust_keys(key["fingerprint"], trust_level)

    def encrypt(
        self, src_path, dest_path, recipients, passphrase=None, always_trust=False
    ):
        with open(src_path, "rb") as f:
            status = self._gpg.encrypt_file(
                file=f,
                recipients=recipients,
                always_trust=always_trust,
                passphrase=passphrase,
                output=dest_path + ".gpg",
            )

        if status.ok is False:
            self.log.error(status.stderr)
            raise CliboaException("Failed to encrypt gpg.")

        self.log.info(status.ok)
        self.log.info(status.status)

    def decrypt(self, src_path, dest_path, passphrase=None, always_trust=False):
        with open(src_path, "rb") as f:
            status = self._gpg.decrypt_file(
                file=f,
                always_trust=always_trust,
                passphrase=passphrase,
                output=dest_path,
            )

        if status.ok is False:
            self.log.error(status.stderr)
            raise CliboaException("Failed to decrypt gpg.")

        self.log.info(status.ok)
        self.log.info(status.status)
