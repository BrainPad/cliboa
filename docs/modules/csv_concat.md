# CsvConcat
Concat plural csv files into one.
This class behaves exactly same with the method 'pandas.concat'.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|src_filenames|File names of source to concat.|No|None|Specify either src_pattern or src_filenames is essential.|
|dest_dir|Path of the directory which is for output files.|No|None||
|dest_name|Output file name|Yes|None||
|encoding|Character encoding when read and write|No|utf-8||

# Examples
```
scenario:
- step: Concat files
  class: CsvConcat
  arguments:
    src_dir: /in
    src_filenames:
      - file1.csv
      - file2.csv
    dest_dir: /out
    dest_name: concat.csv

Input: /in/test1.csv
id, name
1, one
2, two

Input: /in/test1.csv
id, name
3, three
4, four

Output: /out/concat.csv
id, name
1, one
2, two
3, three
4, four
```