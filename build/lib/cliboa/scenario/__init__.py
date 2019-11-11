#
# Copyright 2019 BrainPad Inc. All Rights Reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
from .base import Stdout, SqliteQueryExecute

from .extract.aws import S3Download
from .extract.file import CsvRead
from .extract.ftp import FtpDownload, FtpDownloadFileDelete
from .extract.sftp import (
    SftpFileExtract,
    SftpDownload,
    SftpDelete,
    SftpDownloadFileDelete,
)
from .extract.http import HttpDownload
from .extract.gcp import (
    BigQueryReadCache,
    BigQueryFileDownload,
    GcsDownload,
    GcsDownloadFileDelete,
)
from .extract.sqlite import SqliteRead, SqliteReadRow

from .transform.file import (
    FileDecompress,
    CsvColsExtract,
    ColumnLengthAdjust,
    DateFormatConvert,
    ExcelConvert,
    CsvMerge,
    CsvHeaderConvert,
)

from .load.aws import S3Upload
from .load.file import CsvWrite
from .load.sftp import SftpFileLoad, SftpUpload
from .load.sqlite import SqliteCreation, CsvReadSqliteCreate
from .load.gcp import BigQueryCreate, GcsFileUpload, CsvReadBigQueryCreate
