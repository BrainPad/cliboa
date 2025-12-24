# SftpDelete
Delete a file via SFTP.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|host|Host name or IP address of a sftp server.|Yes|None||
|user|User name for authentication|Yes|None||
|password|Password for authentication|No|None|Either password or key is required|
|key|Path to key for authentication|No|None||
|passphrase|Used for decrypting key|No|None||
|port|Port number of a sftp server|No|22||
|src_dir|Directory of source to download|Yes|None||
|src_pattern|File pattern of source to download. Regexp is available.|Yes|None||
|timeout|Timeout period of sftp connection. Unit is seconds.|No|30||
|retry_count|Retry count of sftp connection|No|3||

# Examples
```
scenario:
- step: Delete a csv file from localhost under /tmp
  class: SftpDelete
  arguments:
    host: localhost
    user: root
    password: password
    src_dir: /root
    src_pattern: *\.csv

- step: Delete a tsv file from 127.0.0.1 under /usr/local
  class: SftpDelete
  arguments:
    host: 127.0.0.1
    user: root
    key: ~/.ssh/id_rsa
    src_dir: /root
    src_pattern: *\.tsv
    timeout: 100
    retry_count: 10
    quit: True
```
