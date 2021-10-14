# HttpDownload
Download a file via HTTP.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_url|URL to download|Yes|None||
|src_pattern|File pattern of source to download. Regexp is not available.|Yes|None|Deprecated|
|dest_dir|Destination directory to download|No|None|
|dest_name|Name of the file which is downloaded to the local directory|Yes|None|
|timeout|Timeout period of sftp connection. Unit is seconds.|No|30||
|retry_count|Retry count of sftp connection|No|3||

# Examples
```
scenario:
- step: Download a svg from brainpad homepage
  class: HttpDownload
  arguments:
    src_url: https://www.brainpad.co.jp
    dest_dir: /tmp
    dest_name: outputs
```