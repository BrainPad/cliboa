# GpgGenerateKey
Generate public and private keys for gpg.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|gnupghome|Home directory of gnupg|Yes|None||
|passphrase||No|None||
|dest_dir|Directory to output key files|Yes|None|If a non-existent directory path is specified, the directory is automatically created.|
|name_real|The real name of the user identity which is represented by the key.|No|None||
|name_email|An email address for the user.|Yes|None||


# Examples
```
- step:
  class: GpgGenerateKey
  arguments:
    gnupghome: /home/resources/gpg
    passphrase: password
    dest_dir: /home/resources/files/keys
    name_email: sample@mail.com
```
