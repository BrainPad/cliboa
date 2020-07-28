# FirestoreDocumentDownload
Download a document from Firestore.

# Parameters
|Parameters|Explanation|Required|Default|Remarks|
|----------|-----------|--------|-------|-------|
|project_id|GCP project id|Yes|None||
|location|GCP location|Yes|None||
|credentials|A service account .json file path or a dictionary containing service account info in Google format|Yes|None||
|collection|Collection name|Yes|None||
|document|Document name|Yes|None||
|dest_dir|Destination directory to download the file|Yes|None||

# Examples
```
- step:
  class: FirestoreDocumentDownload
  arguments:
    project_id: test_gcp
    location: asia-northeast1
    credentials: /root/gcp_credential.json
    collection: user
    document: john_001
    dest_dir: /user
```
