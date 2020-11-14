# FirestoreDocumentCreate
Create document on Firestore.
Document names will be the same with the file names.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|project_id|GCP project id|Yes|None||
|location|GCP location|Yes|None||
|credentials|A service account .json file path or a dictionary containing service account info in Google format|Yes|None||
|collection|Collection name|Yes|None||
|src_dir|Directory that files exists|Yes|None||
|src_pattern|File pattern of source. Regexp is available|Yes|None||

# Examples
```
- step:
  class: FirestoreDocumentCreate
  arguments:
    project_id: test_gcp
    location: asia-northeast1
    credentials: /root/gcp_credential.json
    collection: user
    src_dir: /user/data
    src_pattern: .*\.json
```
