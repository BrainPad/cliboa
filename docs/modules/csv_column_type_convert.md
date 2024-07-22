# CsvColumnTypeConvert
Convert the type of specific column in a csv file.

# Parameters
| Parameters  | Explanation                                          | Required | Default | Remarks                                                                                |
|-------------|------------------------------------------------------|----------|---------|----------------------------------------------------------------------------------------|
| src_dir     | Path of the directory which target files are placed. | Yes      | None    |                                                                                        |
| src_pattern | Regex which is to find target files.                 | Yes      | None    |                                                                                        |
| dest_dir    | Path of the directory which is for output files.     | Yes      | None    | If a non-existent directory path is specified, the directory is automatically created. |
| column      | Type conversion target column.                       | Yes      | None    |                                                                                        |
| type        | Type of the converted data.                          | Yes      | None    | Specify a valid value for the dtype of 'pandas.DataFrame.astype'.                      |

# Examples
```
scenario:
- step: Column Type Convert
  class: CsvColumnTypeConvert
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    column:
      - number
    type: float
    dest_dir: /out

Input: /in/test.csv
id, name, number
1, test, 1

Output: /out/test.csv
id, name, number
1, test, 1.0
```
