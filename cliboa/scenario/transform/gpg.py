import os
from cliboa.core.validator import EssentialParameters
from cliboa.scenario.base import BaseStep
from cliboa.util.gpg import Gpg


class GpgBase(BaseStep):
    def __init__(self):
        super().__init__()
        self._gnupghome = None
        self._passphrase = None
        self._src_dir = None
        self._src_pattern = None
        self._dest_dir = None
        self._always_trust = False
        self._key_dir = None
        self._key_pattern = None
        self._trust_level = None

    def gnupghome(self, gnupghome):
        self._gnupghome = gnupghome

    def passphrase(self, passphrase):
        self._passphrase = passphrase

    def src_dir(self, src_dir):
        self._src_dir = src_dir

    def src_pattern(self, src_pattern):
        self._src_pattern = src_pattern

    def dest_dir(self, dest_dir):
        self._dest_dir = dest_dir

    def always_trust(self, always_trust):
        self._always_trust = always_trust

    def key_dir(self, key_dir):
        self._key_dir = key_dir

    def key_pattern(self, key_pattern):
        self._key_pattern = key_pattern

    def trust_level(self, trust_level):
        self._trust_level = trust_level

    def execute(self, *args):
        valid = EssentialParameters(self.__class__.__name__, [self._gnupghome])
        valid()

    def key_import(self, gpg, key_files, trust_level=None):
        """
        Import key.
        Also set trust level, if parameter "trust_level" is given.
        """
        for file in key_files:
            gpg.import_key(file)
            if trust_level:
                gpg.trust_key(file, trust_level)


class GpgGenerateKey(GpgBase):
    """
    Generate rsa key.
    """

    def __init__(self):
        super().__init__()

        self._name_real = None
        self._name_email = None

    def name_real(self, name_real):
        self._name_real = name_real

    def name_email(self, name_email):
        self._name_email = name_email

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(
            self.__class__.__name__, [self._dest_dir, self._name_email])
        valid()

        Gpg(self._gnupghome).generate_key(
            self._dest_dir,
            name_real=self._name_real,
            name_email=self._name_email,
            passphrase=self._passphrase,
        )


class GpgEncrypt(GpgBase):
    """
    Gpg encryption.
    """

    def __init__(self):
        super().__init__()
        self._recipients = []

    def recipients(self, recipients):
        self._recipients = recipients

    def execute(self, *args):
        super().execute()

        valid = EssentialParameters(self.__class__.__name__, [self._recipients])
        valid()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        if len(files) == 0:
            self._logger.info("No files are found. Nothing to do.")
            return

        gpg = Gpg(self._gnupghome)

        if self._key_dir and self._key_pattern:
            key_files = super().get_target_files(self._key_dir, self._key_pattern)
            self._logger.info("Keys found %s" % key_files)
            self.key_import(gpg, key_files, self._trust_level)

        for file in files:
            dest_path = (
                os.path.join(self._dest_dir, os.path.basename(file))
                if self._dest_dir is not None
                else os.path.join(self._src_dir, os.path.basename(file))
            )
            gpg.encrypt(
                file,
                dest_path,
                recipients=self._recipients,
                passphrase=self._passphrase,
                always_trust=self._always_trust,
            )


class GpgDecrypt(GpgBase):
    """
    Gpg decryption.
    """

    def __init__(self):
        super().__init__()

    def execute(self, *args):
        super().execute()

        files = super().get_target_files(self._src_dir, self._src_pattern)
        if len(files) == 0:
            self._logger.info("No files are found. Nothing to do.")
            return

        if self._key_dir and self._key_pattern:
            key_files = super().get_target_files(self._key_dir, self._key_pattern)
            self._logger.info("Keys found %s" % key_files)
            self.key_import(key_files, self._trust_level)

        gpg = Gpg(self._gnupghome)
        for file in files:
            root, ext = os.path.splitext(file)
            if ext == ".gpg":
                dest_path = (
                    os.path.join(self._dest_dir, os.path.basename(root))
                    if self._dest_dir is not None
                    else os.path.join(self._src_dir, os.path.basename(root))
                )
                gpg.decrypt(
                    file,
                    dest_path,
                    passphrase=self._passphrase,
                    always_trust=self._always_trust,
                )
            else:
                self._logger.warning("Extention was not gpg. %s" % file)
