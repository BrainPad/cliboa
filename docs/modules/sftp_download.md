# SftpDownload
Download a file via SFTP.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|host|Host name or IP address of a sftp server.|Yes|None||
|user|User name for authenticatation|Yes|None||
|password|Password for authentication|No|None|Either password or key is required|
|key|Path to key for authentication|No|None||
|port|Port number of a sftp server|No|22||
|src_dir|Directory of source to download|Yes|None||
|src_pattern|File pattern of source to download. Regexp is available.|Yes|None||
|dest_dir|Destination directory to download.|No|None|
|timeout|Timeout period of sftp connection. Unit is seconds.|No|30||
|retry_count|Retry count of sftp connection.|No|3||
|quit|True or False flag for quitting cliboa process when source files do not exist.|No|False||

# Examples
```
scenario:
- step: Download a csv file from localhost under /tmp
  class: SftpDownload
  arguments:
    host: localhost
    user: root
    password: password
    src_dir: /root
    src_pattern: *\.csv
    dest_dir: /tmp

- step: Download a tsv file from 127.0.0.1 under /usr/local
  class: SftpDownload
  arguments:
    host: 127.0.0.1
    user: root
    key: ~/.ssh/id_rsa
    src_dir: /root
    src_pattern: *\.tsv
    dest_dir: /usr/local
    timeout: 100
    retry_count: 10
    quit: True
```