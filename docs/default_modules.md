# Default Modules
Can use modules as below by default.

## Extract Modules
|Step Class Name|Role|
|----------|-----------|
|[SftpDownload](/docs/modules/sftp_download.md)|Download a file via sftp|
|[SftpDelete](/docs/modules/sftp_delete.md)|Delete a file via sftp|
|[SftpDownloadFileDelete](/docs/modules/sftp_download_file_delete.md)|Delete the downloaded file via sftp|
|[S3Download](/docs/modules/s3_download.md)|Download files from S3|
|[FtpDownload](/docs/modules/ftp_download.md)|Download a file via ftp|
|[FtpDownloadFileDelete](/docs/modules/ftp_download_file_delete.md)|Delete the downloaded file via ftp|
|[GcsDownload](/docs/modules/gcs_download.md)|Download files from GCS|
|[GcsDownloadFileDelete](/docs/modules/gcs_download_file_delete.md)|Delete the downloaded files from GCS|
|[HttpDownload](/docs/modules/http_download.md)|Download a file via http|
|[SqliteReadRow](/docs/modules/sqlite_read_row.md)|Execute query and call result handler|
|[BigQueryReadCache](/docs/modules/bigquery_read_cache.md)|Read from bigquery table|
|[BigQueryFileDownload](/docs/modules/bigquery_file_download.md)|Execute select query and download result as a csv file|


## Transform Modules
|Step Class Name|Role|
|----------|-----------|
|[FileDecompress](/docs/modules/file_decompress.md)|Decompress a file|
|[CsvColsExtract](/docs/modules/csv_cols_extract.md)|Remove specific columns from a csv file.|
|[ColumnLengthAdjust](/docs/modules/column_length_adjust.md)|Adjust columns of a csv file or a tsv file to the specified length.|
|[DateFormatConvert](/docs/modules/date_format_convert.md)|Convert date format of columns of a csv file to another date format|
|[ExcelConvert](/docs/modules/excel_convert.md)|Convert a excel file to a csv file|
|[CsvMerge](/docs/modules/csv_merge.md)|Merge two csv files to a csv file|
|[CsvHeaderConvert](/docs/modules/csv_header_convert.md)|Convert headers of a csv file|
|[FileRename](/docs/modules/file_rename.md)|Change file names with adding either prefix or suffix|


## Load Modules
|Step Class Name|Role|
|----------|-----------|
|[SftpUpload](/docs/modules/sftp_upload.md)|Upload a file via sftp|
|[S3Upload](/docs/modules/s3_upload.md)|Upload files to S3|
|[CsvWrite](/docs/modules/csv_write.md)|Write contents read by 'io: input' to a csv file|
|[CsvReadBigQueryCreate](/docs/modules/csv_read_bigquery_create.md)|Read content from a file and insert it into a table of bigquery|
|[CsvReadSqliteCreate](/docs/modules/csv_read_sqlite_create.md)|Read content from a file and insert it into a table of sqlite|
|[GcsFileUpload](/docs/modules/gcs_file_upload.md)|Upload files to GCS|


## Other Modules
|Step Class Name|Role|
|----------|-----------|
|[Stdout](/docs/modules/stdout.md)|Print contents read by 'io: input' to stdout|
|[SqliteQueryExecute](/docs/modules/sqlite_query_execute.md)|Execute query against sqlite tablecontent from|
