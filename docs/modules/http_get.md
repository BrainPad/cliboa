# HttpGet
A module for executing GET request via HTTP.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_url|URL to download|Yes|None||
|src_pattern|File pattern of source to download. Regexp is not available.|Yes|None|Deprecated|
|dest_dir|Destination directory to download|Yes|None|If a non-existent directory path is specified, the directory is automatically created.|
|dest_name|Name of the file which is downloaded to the local directory|Yes|None|
|timeout|Timeout period of http request. Unit is seconds.|No|30||
|retry_count|Retry count of http request.|No|3||
|retry_intvl_sec|Retry interval of http request. Unit is seconds.|No|10||
|basic_auth|Whether to connect using basic authentication.|No||
|user|User name for basic auth|No|None|Required if Basic authentication is used.|
|password|Password for basic auth|No|None|Required if Basic authentication is used.|

# Examples
```
scenario:
- step:
  class: HttpGet
  arguments:
    src_url: https://www.brainpad.co.jp
    dest_dir: /tmp
    dest_name: outputs
```