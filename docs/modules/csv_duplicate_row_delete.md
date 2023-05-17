# CsvDuplicateRowDelete
Remove duplicate lines in CSV(TSV) file.  
Considered to be deleted only when the entire line is completely matched.  
The order remains the same.  

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|delimiter|Delimiter.|No|","||
|dest_dir|Path of the directory which is for output files.|No|None|If this parameter is not set, the file is created in the same directory as the processing file. If a non-existent directory path is specified, the directory is automatically created.|

# Example1
```
scenario:
- step: dulicate row delete
  class: CsvDuplicateRowDelete
  arguments:
    src_dir: /in
    src_pattern: test\.csv
    dest_dir: /out

Input: /in/test.csv
id, name, passwd, memo 
1, spam1, spampass1, memo1
1, spam1, spampass1, memo1
2, spam2, spampass2, memo2

Output: /out/test.csv
id, memo, name, passwd
1, memo1, spam1, spampass1
2, memo2, spam2, spampass2
```

# Example2
```
scenario:
- step: dulicate row delete
  class: CsvDuplicateRowDelete
  arguments:
    src_dir: /in
    src_pattern: test\.tsv
    dest_dir: /out

Input: /in/test.tsv
id\tname\tpasswd\tmemo 
1\tspam1\tspampass1\tmemo1"
1\tspam1\tspampass1\tmemo1"
2\tspam2\tspampass2\tmemo2"

Output: /out/test.tsv
id, memo, name, passwd
id\tname\tpasswd\tmemo 
1\tspam1\tspampass1\tmemo1"
2\tspam2\tspampass2\tmemo2"
```
