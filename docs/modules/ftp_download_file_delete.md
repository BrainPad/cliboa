# FtpDownloadFileDelete
Remove files downloaded via class 'FtpDownload' from FTP server.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|symbol|Symbol defined against FtpDownload|Yes|None||

# Examples
```
scenario:
- step: ftp_download
  class: FtpDownload
  arguments:
    host: localhost
    user: root
    password: password
    src_dir: /root
    src_pattern: *\.tsv
    dest_dir: /tmp

- step: Delete the tsv file downloaded from localhost
  class: FtpDownloadFileDelete
  symbol: ftp_download
```
