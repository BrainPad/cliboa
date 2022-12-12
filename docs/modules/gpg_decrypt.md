# GpgDecrypt
Gpg decrypt.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|gnupghome|Home directory of gnupg|Yes|None||
|passphrase||No|None||
|src_dir|Directory that files exists|Yes|None||
|src_pattern|File pattern|Yes|None||
|dest_dir|Directory to output decrypted files|No|None|If not given, decrypted files are created with the same directory to src_dir. If a non-existent directory path is specified, the directory is automatically created.|
|always_trust|Skip key validation and assume that used keys are always fully trusted.|No|False||
|key_dir|Directory that rsa key files exists|No|None||
|key_pattern|Rsa keys file pattern|No|None||
|trust_level|Trust level for imported keys|No|None|One of the followings are allowed [TRUST_UNDEFINED, TRUST_NEVER, TRUST_MARGINAL, TRUST_FULLY, TRUST_ULTIMATE]|


# Examples
```
- step:
  class: GpgDecrypt
  arguments:
    gnupghome: /home/resources/gpg
    passphrase: password
    src_dir: /home/resources/files/in
    src_pattern: .*\.txt\.gpg
    dest_dir: /home/resources/files/out
    key_dir: /home/resources/keys
    key_pattern: rsa_public_key
    trust_level: TRUST_ULTIMATE
```
