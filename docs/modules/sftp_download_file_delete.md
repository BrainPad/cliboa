# SftpDownloadFileDelete
Remove files downloaded via class 'SftpDownload' from SFTP server.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|symbol|Symbol defined against SftpDownload|Yes|None||

# Examples
```
scenario:
- step: sftp_download
  class: SftpDownload
  arguments:
    host: localhost
    user: root
    password: password
    src_dir: /root
    src_pattern: *\.tsv
    dest_dir: /tmp

- step: Delete the tsv file downloaded from localhost
  class: SftpDownloadFileDelete
  symbol: sftp_download
```
