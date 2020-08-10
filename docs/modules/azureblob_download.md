# AzureBlobDownload

Download files from Azure Blob Storage.

# Parameters

| Parameters         | Explanation                                             | Required | Default | Remarks |
| ------------------ | ------------------------------------------------------- | -------- | ------- | ------- |
| account_url        | Azure account URL                                       | No       | None    |         |
| account_access_key | Azure access key                                        | No       | None    |         |
| connection_string  | Connection string (overrides above two parameters)      | No       | None    |         |
| container_name     | Blob container name                                     | Yes      |         |         |
| prefix             | Folder prefix used to filter blobs                      | No       | None    |         |
| src_pattern        | File pattern of source to download. Regexp is available | Yes      |         |         |
| dest_dir           | Destination directory to download                       | Yes      |         |         |

`account_url` is the URL to the Azure blob storage account and can be optionally authenticated with a SAS token. `account_access_key` can be a SAS token string or an account shared access key (optional). Instead of them, a [connection string](https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string) can be also specified.

# Examples

```
- step:
  class: AzureBlobDownload
  arguments:
    account_url: "https://<my_account_name>.blob.core.windows.net"
    account_access_key: "<account_access_key>"
    container_name: test
    prefix: test
    src_pattern: test_(.*)\.csv
    dest_dir: /tmp
```
