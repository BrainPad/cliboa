MODULES = [
    {
        "step": None,
        "class": "AzureBlobDownload",
        "arguments": {
            "container_name": "",
            "src_pattern": "",
            "dest_dir": ""
        }
    },
    {
        "step": None,
        "class": "AzureBlobUpload",
        "arguments": {
            "container_name": "",
            "src_dir": "",
            "src_pattern": "",
            "dest_dir": ""
        }
    },
    {
        "step": None,
        "class": "BigQueryFileDownload",
        "arguments": {
            "project_id": "",
            "location": "",
            "dataset": "",
            "tblname": "",
            "bucket": "",
            "dest_dir": ""
        }
    },
    {
        "step": None,
        "class": "BigQueryRead",
        "arguments": {
            "project_id": "",
            "location": "",
            "dataset": "",
            "tblname": ""
        }
    },
    {
        "step": None,
        "class": "BigQueryWrite",
        "arguments": {
            "src_dir": "",
            "src_pattern": "",
            "project_id": "",
            "location": "",
            "dataset": "",
            "tblname": "",
            "table_schema": ""
        }
    },
    {
        "step": None,
        "class": "ColumnLengthAdjust",
        "arguments": {
            "src_dir": "",
            "src_pattern": "",
            "adjust": ""
        }
    },
    {
        "step": None,
        "class": "ColumnConcat",
        "arguments": {
            "src_dir": "",
            "src_pattern": "",
            "columns": "",
            "dest_column_name": ""
        }
    },
    {
        "step": None,
        "class": "CsvColumnExtract",
        "arguments": {
            "src_dir": "",
            "src_pattern": ""
        }
    },
    {
        "step": None,
        "class": "CsvColumnHash",
        "arguments": {
            "src_dir": "",
            "src_pattern": ""
        }
    },
    {
        "step": None,
        "class": "CsvConcat",
        "arguments": {
            "src_dir": "",
            "src_pattern": "",
            "dest_name": ""
        }
    },
    {
        "step": None,
        "class": "CsvConvert",
        "arguments": {
            "src_dir",
            "src_pattern",
            "before_format",
            "before_enc"
        }
    },
    {
        "step": None,
        "class": "CsvFormatChange",
        "arguments": {
            "src_dir": "",
            "src_pattern": "",
            "before_format": "",
            "before_enc": "",
            "after_format": "",
            "after_enc": ""
        }
    },
    {
        "step": None,
        "class": "CsvHeaderConvert",
        "arguments": {
            "src_dir": "",
            "src_pattern": "",
            "headers": []
        }
    },
    {
        "step": None,
        "class": "CsvReadSqliteCreate",
        "arguments": {
            "src_dir": "",
            "src_pattern": "",
            "dbname": "",
            "tblname": ""
        }
    },
    {
        "step": None,
        "class": "CsvSort",
        "arguments": {
            "src_dir": "",
            "src_pattern": "",
            "order": ""
        }
    },
    {
        "step": None,
        "class": "CsvToJson",
        "arguments": {
            "src_dir": "",
            "src_pattern": ""
        }
    },
    {
        "step": None,
        "class": "CsvWrite",
        "arguments": {
            "io": "",
            "dest_path": ""
        }
    },
    {
        "step": None,
        "class": "DateFormatConvert",
        "arguments": {
            "src_dir": "",
            "src_pattern": ""
        }
    },
    {
        "step": None,
        "class": "ExcelConvert",
        "arguments": {
            "src_dir": "",
            "src_pattern": ""
        }
    },
    {
        "step": None,
        "class": "FileArchive",
        "arguments": {
            "src_dir": "",
            "src_pattern": "",
            "dest_name": "",
            "format": ""
        }
    },
    {
        "step": None,
        "class": "FileCompress",
        "arguments": {
            "src_dir": "",
            "src_pattern": "",
            "format": ""
        }
    },
    {
        "step": None,
        "class": "FileConvert",
        "arguments": {
            "src_dir": "",
            "src_pattern": "",
            "encoding_from": "",
            "encoding_to": ""
        }
    },
    {
        "step": None,
        "class": "FileDecompress",
        "arguments": {
            "src_dir": "",
            "src_pattern": "",
            "dest_dir": ""
        }
    },
    {
        "step": None,
        "class": "FileDivide",
        "arguments": {
            "src_dir": "",
            "src_pattern": "",
            "divide_rows": ""
        }
    },
    {
        "step": None,
        "class": "FileRename",
        "arguments": {
            "src_dir": "",
            "src_pattern": ""
        }
    },
    {
        "step": None,
        "class": "FirestoreDocumentCreate",
        "arguments": {
            "project_id": "",
            "location": "",
            "collection": "",
            "src_dir": "",
            "src_pattern": ""
        }
    },
    {
        "step": None,
        "class": "FirestoreDocumentDownload",
        "arguments": {
            "project_id": "",
            "location": "",
            "collection": "",
            "document": "",
            "dest_dir": ""
        }
    },
    {
        "step": None,
        "class": "FtpDownload",
        "arguments": {
            "host": "",
            "user": "",
            "src_dir": "",
            "src_pattern": ""
        }
    },
    {
        "step": None,
        "class": "",
        "arguments": {
            "": "",
            "": "",
            "": "",
            "": "",
            "": "",
        }
    },
    {
        "step": None,
        "class": "",
        "arguments": {
            "": "",
            "": "",
            "": "",
            "": "",
            "": "",
        }
    },
    {
        "step": None,
        "class": "",
        "arguments": {
            "": "",
            "": "",
            "": "",
            "": "",
            "": "",
        }
    },
    {
        "step": None,
        "class": "",
        "arguments": {
            "": "",
            "": "",
            "": "",
            "": "",
            "": "",
        }
    },
]