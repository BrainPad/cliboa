# ExcelConvert
Convert a excel file to a csv file.
This class behaves as follows
1. Call method pandas.read_excel to change the sheet to DataFrame
2. Then call method pandas.to_csv to change from DataFrame to csv.
Which means, currently only excel to csv is supported. 

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None|If this parameter is not set, the file is created in the same directory as the processing file. If a non-existent directory path is specified, the directory is automatically created.|
|encoding|Character encoding when read and write|No|utf-8||
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Examples
```
scenario:
- step: Convert an excel file
  class: ExcelConvert
  arguments:
    src_dir: /in
    src_pattern: test\.xlsx
    dest_dir: /out

Input: /in/test.xlsx
Output: /out/test.csv
```