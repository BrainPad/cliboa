# Stdout
Print contents read by 'io: input' to stdout. Basically use together with an extract module.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|io|Types of I/O. Must specify 'output'.|Yes|None||


# Examples
```
scenario:
- step:
  class: CsvRead
  io: input
  arguments:
    src_path: /root/sample*.csv
    columns:
      - column1
      - column2
- step: Print contents read by CsvWrite to stdout
  class: Stdout
  io: output
```