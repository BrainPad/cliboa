# CsvMergeExclusive
Compare specific columns each file. 
If matched, exclude rows.

# Parameters
| Parameters          | Explanation                                          | Required | Default | Remarks                                                                                                                                                                                |
|---------------------|------------------------------------------------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| src_dir             | Path of the directory which target files are placed. | Yes      | None    |                                                                                                                                                                                        |
| src_pattern         | Regex which is to find target files.                 | Yes      | None    |                                                                                                                                                                                        |
| dest_dir            | Path of the directory which is for output files.     | No       | None    | If this parameter is not set, the file is created in the same directory as the processing file. If a non-existent directory path is specified, the directory is automatically created. |
| src_column          | Compare target column for "src_dir" and "src_path".  | Yes      | None    | Specify only one column.                                                                                                                                                               |
| target_compare_path | Path of the file which target for comparison.        | Yes      | None    |                                                                                                                                                                                        |
| target_column       | Compare target column for "target_compare_path".     | Yes      | None    | Specify only one column.                                                                                                                                                               |
| all_column          | Delete rows when all column values match.            | No       | False   | src_column and target_column cannot be used together when all_column is "True".                                                                                                          |
| encoding            | Character encoding when read and write               | No       | utf-8   |                                                                                                                                                                                        |

# Example 1
```
scenario:
- step:
  class: CsvMergeExclusive
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    src_column: data
    target_compare_path: /in/compare.csv
    target_column: key
    dest_dir: /out

Input: /in/test.csv
id, name, data
1, one, first
2, two, second
3, three, third

Input Compare Target: /in/compare.csv
key, value
first, test1
fourth, test4

Output: /out/test.csv
id, name, data
2, two, second
3, three, third
```

# Example 2
```
scenario:
- step:
  class: CsvMergeExclusive
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    target_compare_path: /in/compare.csv
    all_column: True
    dest_dir: /out

Input: /in/test.csv
id, name, data
1, one, first
2, two, second
3, three, third

Input Compare Target: /in/compare.csv
id, name, data
1, one, first
2, two, secondary
3, three, third
4, four, fourth

Output: /out/test.csv
id, name, data
2, two, second
```