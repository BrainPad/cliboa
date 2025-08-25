# CsvSplit
Split csv files by specified method.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which input files are places.|Yes|None||
|src_pattern|File pattern of source csv files. Regexp is available.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None|If a non-existent directory path is specified, the directory is automatically created.|
|method|Split method.|Yes|None|Only `rows` or `grouped` can be specified.|
|rows|When method is `rows`, split every N rows.|No|None|Required when method is `rows`|
|suffix_format|When method is `rows`, output file's suffix.(used in python's str.format)|No|None||
|key_column|When method is `grouped`, column name to use grouped split.|No|None|Required when method is `grouped`|
|encoding|Character encoding when read and write|No|utf-8||

# Examples

## Method: rows
```
scenario:
- step: Split file by rows
  class: CsvSplit
  arguments:
    src_dir: /in
    src_pattern: test.csv
    dest_dir: /out
    method: rows
    rows: 3

Input: /in/test.csv
no, name
1, alpha
2, beta
3, gamma
4, delta
5, epsilon

Output: /out/test.00.csv
no, name
1, alpha
2, beta
3, gamma

Output: /out/test.01.csv
no, name
4, delta
5, epsilon
```


## Method: grouped
```
scenario:
- step: Split file on grouped by class column's value
  class: CsvSplit
  arguments:
    src_dir: /in
    src_pattern: test.csv
    dest_dir: /out
    method: grouped
    key_column: class

Input: /in/test.csv
name, class
alpha, A
beta, B
gamma, A
delta, B
epsilon, C

Output: /out/A.csv
name, class
alpha, A
gamma, A

Output: /out/B.csv
name, class
beta, B
delta, B

Output: /out/C.csv
name, class
epsilon, C
```
