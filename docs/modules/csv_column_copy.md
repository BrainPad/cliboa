# CsvColumnCopy
Copy column data (new or overwrite).

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None||
|encoding|Character encoding when read and write.|No|utf-8||
|src_column|Copy source column|Yes|None||
|dest_column|Destination column|Yes|None|Overwrite columns if they already exist.|
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Examples
```
scenario:
- step:
  class: CsvColumnCopy
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    src_column: address
    dest_comumn: mail

Input: /in/test.csv
id, name, address
1, test, test@aaa.com

Output: /out/test.csv
id, name, address, mail
1, test, test@aaa.com, test@aaa.com
```
