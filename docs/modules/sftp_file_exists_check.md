# SftpFileExistsCheck
File check via SFTP.

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
|retry_count|Retry count of sftp connection.|No|3||
|ignore_empty_file|If True, size zero files are not be downloaded|No|False||

# Examples
```
- step: File check a tsv file from xxx.xxx.xxx.xxx 
  class: SftpFileExistsCheck
  arguments:
    host: xxx.xxx.xxx.xxx
    user: xxxxx
    key: ~/.ssh/id_rsa
    src_dir: /root
    src_pattern: *\.tsv
    timeout: 100
    retry_count: 10

- step: Embed contents of key at scenario.yml
  class: SftpFileExistsCheck
  arguments:
    host: xxx.xxx.xxx.xxx
    user: xxxxx
    key:
      content: |
        -----BEGIN RSA PRIVATE KEY-----
        .......
        -----END RSA PRIVATE KEY-----
    passphrase: xxxxx
    src_dir: /root
    src_pattern: *\.tsv
    timeout: 100
    retry_count: 10
```