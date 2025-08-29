# CsvRowDelete
Filter CSV rows by comparing key column values between source and reference CSV files. Rows are kept or deleted based on whether their key values exist in the reference file.

# Parameters
| Parameters       | Explanation                                                                                                                 | Required | Default | Remarks |
|------------------|-----------------------------------------------------------------------------------------------------------------------------|----------|---------|---------|
| src_dir          | Path of the directory which target files are placed.                                                                        | Yes      | None    |         |
| src_pattern      | Regex which is to find target files.                                                                                        | Yes      | None    |         |
| alter_path       | Csv file path to compare.                                                                                                   | Yes      | None    |         |
| src_key_column   | Column and regular expression pair.                                                                                         | Yes      | None    |         |
| alter_key_column | Whether an error is thrown when files are not found in src_dir.                                                             | No       | None    |         |
| delimiter        | Delimiter, used with prefix to emulate hierarchy.                                                                           | No       | None    |         |
| has_match        | Specify True if you want to delete when the values are the same, False if you want to delete when the values are different. | Yes      | True    |         |

# Example 1
```
scenario:
- step:
  class: CsvRowDelete
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    alter_path: /alter/test2.csv
    src_key_column: col_1
    alter_key_column: col_3

Input: /in/test.csv
col_1, col_2
1, one
2, two

Input: /alter/test2.csv
col_3, col_4
1, uno
3, tres

Output: /in/test.csv
col_1, col_2
2, two
```

# Example 2
```
scenario:
- step:
  class: CsvRowDelete
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    alter_path: /alter/test2.csv
    src_key_column: col_1
    alter_key_column: col_3
    has_match: False

Input: /in/test.csv
col_1, col_2
1, one
2, two

Input: /alter/test2.csv
col_3, col_4
1, uno
3, tres

Output: /in/test.csv
col_1, col_2
1, one
```