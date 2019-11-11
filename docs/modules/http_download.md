# HttpDownload
Download a file via HTTP.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_url|URL to download|Yes|None||
|src_pattern|File pattern of source to download. Regexp is not available.|Yes|None||
|dest_dir|Destination directory to download|No|None|
|timeout|Timeout period of sftp connection. Unit is seconds.|No|30||
|retry_count|Retry count of sftp connection|No|3||

# Examples
```
scenario:
- step: Download a svg from brainpad homepage
  class: HttpDownload
  arguments:
    src_url: https://www.brainpad.co.jp
    src_pattern: /common/images/logo-header.svg
    dest_dir: /tmp
```