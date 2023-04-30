# AesDecrypt
Decrypt files encrypted with aes method.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Directory that files exists|Yes|None||
|src_pattern|File pattern|Yes|None||
|dest_dir|Directory to output encrypted files|No|None|If not given, decrypted files are created with the same directory to src_dir|
|key_dir|Directory that aes key files exists|Yes|None||
|key_pattern|Aes keys file pattern|Yes|None||


# Examples
```
- step:
  class: AesDecrypt
  arguments:
    src_dir: /in
    src_pattern: test\.txt
    dest_dir: /out
    key_dir: /in
    key_pattern: test\.key
```
