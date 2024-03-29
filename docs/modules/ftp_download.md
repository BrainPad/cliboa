# FtpDownload
Download a file via FTP.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|host|Host name or IP address of a sftp server.|Yes|None||
|user|User name for authentication|Yes|None||
|password|Password for authentication|No|None|Either password or key is required|
|key|Path to key for authentication|No|None||
|port|Port number of a sftp server|No|21||
|src_dir|Directory of source to download|Yes|None||
|src_pattern|File pattern of source to download. Regexp is available.|Yes|None||
|dest_dir|Destination directory to download.|No|`""`|If a non-existent directory path is specified, the directory is automatically created.|
|timeout|Timeout period of sftp connection. Unit is seconds.|No|30||
|retry_count|Retry count of sftp connection|No|3||
|tls|True or False flag for secure connection. If True, FTPS is used as protocol.|No|False||

# Examples
```
scenario:
- step: Download a csv file from localhost under /tmp
  class: FtpDownload
  arguments:
    host: localhost
    user: root
    password: password
    src_dir: /root
    src_pattern: *\.csv
    dest_dir: /tmp

- step: Download a tsv file from localhost under /usr/local by using FTPS.
  class: FtpDownload
  arguments:
    host: localhost
    user: root
    key: ~/.ssh/id_rsa
    src_dir: /root
    src_pattern: *\.tsv
    dest_dir: /usr/local
    timeout: 100
    retry_count: 10
    tls: True
```