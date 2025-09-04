# CsvDuplicateRowDelete
Remove duplicate lines in CSV(TSV) file.  
Considered to be deleted only when the entire line is completely matched.  

**Performance Improvements (v2.0+):**
- Memory-efficient processing using set-based duplicate detection
- ~32x faster processing on datasets with many duplicates
- Support for very large files through chunked processing and Dask engine

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|src_dir|Path of the directory which target files are placed.|Yes|None||
|src_pattern|Regex which is to find target files.|Yes|None||
|delimiter|Delimiter.|No|","||
|dest_dir|Path of the directory which is for output files.|No|None|If this parameter is not set, the file is created in the same directory as the processing file. If a non-existent directory path is specified, the directory is automatically created.|
|engine|Processing engine.|No|"pandas"|**pandas**: Memory-efficient processing with preserved row order. **dask**: For very large files (may not preserve original row order but provides better memory efficiency).|

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

# Example2: TSV Processing
```
scenario:
- step: dulicate row delete
  class: CsvDuplicateRowDelete
  arguments:
    src_dir: /in
    src_pattern: test\.tsv
    dest_dir: /out
    delimiter: "\t"

Input: /in/test.tsv
id\tname\tpasswd\tmemo 
1\tspam1\tspampass1\tmemo1"
1\tspam1\tspampass1\tmemo1"
2\tspam2\tspampass2\tmemo2"

Output: /out/test.tsv
id\tname\tpasswd\tmemo 
1\tspam1\tspampass1\tmemo1"
2\tspam2\tspampass2\tmemo2"
```

# Example3: Large File Processing with Dask Engine
```
scenario:
- step: large file duplicate removal
  class: CsvDuplicateRowDelete
  arguments:
    src_dir: /data
    src_pattern: large_dataset\.csv
    dest_dir: /output
    engine: dask

# For very large files (GB+), use dask engine for better memory efficiency
# Note: Row order may not be preserved with dask engine
```

# Performance Characteristics

| Engine | Memory Usage | Processing Speed | Row Order | Best Use Case |
|--------|--------------|------------------|-----------|---------------|
| pandas | Low | High | Preserved | Most CSV files, when order matters |
| dask | Very Low | Moderate | Not guaranteed | Very large files (GB+), when order doesn't matter |

**Tip**: Start with the default `pandas` engine. Switch to `dask` only for extremely large files that cause memory issues.
