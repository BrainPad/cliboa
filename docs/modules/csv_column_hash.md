# CsvColumnHash
Hash columns of a csv file with SHA256

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|dest_dir|Path of the directory which is for output files.|No|None||
|encoding|Character encoding when read and write|No|utf-8||
|columns|Csv column names which hash with SHA256|Yes|None||
|nonfile_error|Whether an error is thrown when files are not found in src_dir.|No|False||

# Examples
```
scenario:
- step:
  class: CsvColumnHash
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    dest_dir: /out
    columns:
      - passwd

Input: /in/test.csv
id, name, passwd
1, spam, spam1234

Output: /out/test.csv
id, name, passwd
1, spam, ec77022924e329f8e01deab92a4092ed8b7ec2365f1e719ac4e9686744341d95
```
