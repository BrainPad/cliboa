# GpgEncrypt
Gpg encrypt.
Encrypted file names will be the same name with original file names with ".gpg" is added at the end.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|gnupghome|Home directory of gnupg|Yes|None||
|passphrase||No|None||
|src_dir|Directory that files exists|Yes|None||
|src_pattern|File pattern|Yes|None||
|dest_dir|Directory to output encrypted files|No|None|If not given, encrypted files are created with the same directory to src_dir|
|recipients|Recipients who can encrypt files|Yes|[]||
|always_trust|Skip key validation and assume that used keys are always fully trusted.|No|False||
|key_dir|Directory that rsa key files exists|No|None||
|key_pattern|Rsa keys file pattern|No|None||
|trust_level|Trust level for imported keys|No|None|One of the followings are allowed [TRUST_UNDEFINED, TRUST_NEVER, TRUST_MARGINAL, TRUST_FULLY, TRUST_ULTIMATE]|


# Examples
```
- step:
  class: GpgEncrypt
  arguments:
    gnupghome: /home/resources/gpg
    passphrase: password
    src_dir: /home/resources/files/in
    src_pattern: .*\.txt
    dest_dir: /home/resources/files/out
    key_dir: /home/resources/keys
    key_pattern: rsa_public_key
    trust_level: TRUST_ULTIMATE
```
