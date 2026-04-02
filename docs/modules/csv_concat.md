# CsvConcat
Concat plural csv files into one.
This class behaves exactly same with the method 'pandas.concat'.

You can concatenate **all** matched files into a single output (`mode: all`, default), or **group** files by output basename derived from regex capturing groups (`mode: group`).

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Conditional|None|Required unless `src_filenames` is specified (deprecated). Used as `fullmatch` against each file **name** (not the full path). With `mode: group`, the pattern must include at least one **capturing** group; see below.|
|src_filenames|Deprecated since v3.1. Use `src_pattern` instead.|No|None|Will be removed in v4.0.|
|dest_dir|Path of the directory which is for output files.|Yes|None|If a non-existent directory path is specified, the directory is automatically created.|
|dest_name|Output file name.|Conditional|None|Required when `mode` is `all`. Must **not** be set when `mode` is `group` (output names come from `src_pattern`).|
|mode|How inputs are combined.|No|`all`|`all`: merge every target file into one file named `dest_name`. `group`: split targets into groups that share the same derived output basename, then write one file per group.|
|encoding|Character encoding when read and write|No|utf-8||


# Examples
```
scenario:
- step: Concat files
  class: CsvConcat
  arguments:
    src_dir: /in
    src_pattern: 'file.*\.csv'
    dest_dir: /out
    dest_name: concat.csv

Input: /in/file1.csv
id, name
1, one
2, two

Input: /in/file2.csv
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

## Group mode example
```
scenario:
- step: Concat files by group
  class: CsvConcat
  arguments:
    src_dir: /in
    src_pattern: '^(.+_names)_\d+(\.csv)$'
    dest_dir: /out
    mode: group

Input: /in/cat_names_00.csv, /in/cat_names_01.csv, /in/dog_names_00.csv
Output: /out/cat_names.csv, /out/dog_names.csv
```
