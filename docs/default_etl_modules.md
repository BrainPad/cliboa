# Default ETL Modules
Can use ETL modules as below by default.

## Extract Modules
|Step Class Name|Role|
|----------|-----------|
|[AzureBlobDownload](https://github.com/cocoa-maemae/cliboa/blob/fix-manual/docs/modules/azureblob_download.md)|Download files from Azure Blob Storage|
|[BigQueryRead](/docs/modules/bigquery_read.md)|Read from bigquery table|
|[FirestoreDocumentDownload](/docs/modules/firestore_document_download.md)|Download a document from Firestore|
|[FtpDownload](/docs/modules/ftp_download.md)|Download a file via ftp|
|[GcsDownload](/docs/modules/gcs_download.md)|Download files from GCS|
|[HttpDownload](/docs/modules/http_download.md)|Download a file via http|
|[SftpDownload](/docs/modules/sftp_download.md)|Download a file via sftp|
|[S3Download](/docs/modules/s3_download.md)|Download files from S3|
|[SqliteReadRow](/docs/modules/sqlite_read_row.md)|Execute query and call result handler|
|[FirestoreDocumentDownload](/docs/modules/firestore_document_download.md)|Download a document from Firestore|


## Transform Modules
|Step Class Name|Role|
|----------|-----------|
|[ColumnLengthAdjust](/docs/modules/column_length_adjust.md)|Adjust columns of a csv file or a tsv file to the specified length.|
|[CsvColumnExtract](/docs/modules/csv_column_extract.md)|Extract specific columns from csv files.|
|[CsvConvert](/docs/modules/csv_convert.md)|Create new csv(tsv) file with given parameters|
|[CsvMerge](/docs/modules/csv_merge.md)|Merge two csv files to a csv file|
|[CsvConcat](/docs/modules/csv_concat.md)|Concat csv files|
|[CsvHeaderConvert](/docs/modules/csv_header_convert.md)|Convert headers of a csv file|
|[DateFormatConvert](/docs/modules/date_format_convert.md)|Convert date format of columns of a csv file to another date format|
|[ExcelConvert](/docs/modules/excel_convert.md)|Convert a excel file to a csv file|
|[FileCompress](/docs/modules/file_compress.md)|Compress a file|
|[FileConvert](/docs/modules/file_convert.md)|Convert file encoding|
|[FileDecompress](/docs/modules/file_decompress.md)|Decompress a file|
|[FileDivide](/docs/modules/file_divide.md)|Divide a file to plural files|
|[FileRename](/docs/modules/file_rename.md)|Change file names with adding either prefix or suffix|


## Load Modules
|Step Class Name|Role|
|----------|-----------|
|[AzureBlobUpload](https://github.com/cocoa-maemae/cliboa/blob/fix-manual/docs/modules/azureblob_upload.md)|Upload files to Azure Blob Storage|
|[CsvWrite](/docs/modules/csv_write.md)|Write contents read by 'io: input' to a csv file|
|[BigQueryWrite](/docs/modules/bigquery_write.md)|Read content from a file and insert it into a table of bigquery|
|[CsvReadSqliteCreate](/docs/modules/csv_read_sqlite_create.md)|Read content from a file and insert it into a table of sqlite|
|[FirestoreDocumentCreate](/docs/modules/firestore_document_create.md)|Create document|
|[GcsUpload](/docs/modules/gcs_upload.md)|Upload files to GCS|
|[SftpUpload](/docs/modules/sftp_upload.md)|Upload a file via sftp|
|[S3Upload](/docs/modules/s3_upload.md)|Upload files to S3|


## Other Modules
|Step Class Name|Role|
|----------|-----------|
|[Stdout](/docs/modules/stdout.md)|Print contents read by 'io: input' to stdout|
|[SftpDelete](/docs/modules/sftp_delete.md)|Delete a file via sftp|
|[SqliteQueryExecute](/docs/modules/sqlite_query_execute.md)|Execute query against sqlite tablecontent from|
