# SftpUpload
Upload a file via SFTP.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|host|Host name or IP address of a sftp server.|Yes|None||
|user|User name for authenticatation|Yes|None||
|password|Password for authentication|No|None|Either password or key is required|
|key|Path to key for authentication|No|None||
|port|Port number of a sftp server|No|22||
|src_dir|Directory of source to upload|Yes|None||
|src_pattern|File pattern of source to upload. Regexp is available.|Yes|None||
|dest_dir|Destination directory to upload.|No|None|
|timeout|Timeout period of sftp connection. Unit is second.|No|30||
|retry_count|retry count of sftp connection.|No|3||

# Examples
```
scenario:
- step: Upload a csv file to localhost
  class: SftpUpload
  arguments:
    host: localhost
    user: root
    password: password
    src_dir: /root
    src_pattern: *\.csv
    dest_dir: /tmp

- step: Upload a tsv file to 127.0.0.1 
  class: SftpUpload
  arguments:
    host: 127.0.0.1
    user: root
    key: ~/.ssh/id_rsa
    src_dir: /root
    src_pattern: *\.tsv
    dest_dir: /usr/local
    timeout: 100
    retry_count: 10
```