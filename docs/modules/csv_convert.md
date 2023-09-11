# Convert csv format
Headers, extension and encoding are changeable.
Plural files can be converted at the same time, but format of all files must be the same.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None|If this parameter is not set, the file is created in the same directory as the processing file. If a non-existent directory path is specified, the directory is automatically created.|
|headers|List of column names which is to be renamed.|No|None|Dict of list ex. [{before_column_name1: after_column_name1}, {before_column_name2: after_column_name2}]. Headers option is enabled when headers_existence is True and add_headers is not specified.|
|headers_existence|Whether to write header at the first row.|No|True|When headers_existence is False, the header is not output even if headers or add_headers is input.|
|add_headers|Describe the headers you want to add in list format.|No|None|Thia parameter works when headers_existence is true. Items in headers are also ignored.|
|before_format|File extension before convert.|Yes|None|"csv" or "tsv"|
|before_enc|File encoding before convert.|Yes|utf-8||
|before_escapechar|File escape char before convert.|No|None|If None, escape with double quotes.|
|after_format|File extension after converted.|No|Same with before_format|"csv" or "tsv"|
|after_enc|File encoding after converted.|Same with before_enc|None||
|after_escapechar|File escape char after convert.|No|None|If None, escape with double quotes.|
|after_nl|New line for converted csv.|No|LF|"LF" or "CR" or "CRLF"|
|reader_quote|quote type for read csv.|No|QUOTE_NONE|"QUOTE_ALL" or "QUOTE_MINIMAL" or "QUOTE_NONNUMERIC" or "QUOTE_NONE"|
|quote|quote type for converted csv.|No|QUOTE_MINIMAL|"QUOTE_ALL" or "QUOTE_MINIMAL" or "QUOTE_NONNUMERIC" or "QUOTE_NONE"|
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Example 1
```
scenario:
- step: format change
  class: CsvConvert
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    before_format: csv
    before_enc: utf-8
    after_format: tsv
    quote: QUOTE_ALL
    headers: [{id: key}]

Input: /in/test.csv
id, name
1, one
2, two

Output: /out/test.tsv
"key",\t"name"
"1"\t"one"
"2"\t"two"
```  

# Example 2
```
scenario:
- step: format change
  class: CsvConvert
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    before_format: csv
    before_enc: utf-8
    after_format: tsv
    quote: QUOTE_ALL
    headers: [{id: key}]
    headers_existence: False

Input: /in/test.csv
id, name
1, one
2, two

Output: /out/test.tsv
"1"\t"one"
"2"\t"two"
```