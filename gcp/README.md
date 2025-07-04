# Google Cloud Storage (GCS) Backup Support

rqlite now supports automatic backups to Google Cloud Storage (GCS) in addition to the existing AWS S3 support.

## Configuration

### Basic GCS Configuration

```json
{
  "version": 1,
  "type": "gcs",
  "interval": "24h",
  "vacuum": true,
  "no_compress": false,
  "timestamp": true,
  "sub": {
    "project_id": "my-project-id",
    "bucket": "rqlite-backups", 
    "path": "production/backup.db",
    "credentials_file": "/path/to/service-account.json"
  }
}
```

### Configuration Fields

**General Fields (same as S3):**
- `version`: Config version (must be 1)
- `type`: Set to "gcs" for Google Cloud Storage
- `interval`: Backup frequency (e.g., "24h", "1h", "30m")
- `vacuum`: Whether to vacuum database before backup
- `no_compress`: Whether to disable compression
- `timestamp`: Whether to add timestamp to backup filenames

**GCS-Specific Fields:**
- `project_id`: Google Cloud project ID
- `bucket`: GCS bucket name
- `path`: Object path/key within the bucket
- `credentials_file`: Path to service account JSON file (optional)
- `credentials_json`: Service account JSON content as string (optional)

### Authentication

GCS authentication supports multiple methods:

1. **Service Account File**: Provide path via `credentials_file`
2. **Service Account JSON**: Provide JSON content via `credentials_json`  
3. **Default Application Credentials**: If neither is provided, uses default GCP credentials

Environment variables are expanded in the configuration file, so you can use:
- `$GOOGLE_APPLICATION_CREDENTIALS` for credentials file path
- `$GCP_PROJECT_ID` for project ID
- `$GCS_BUCKET` for bucket name

### Example with Environment Variables

```json
{
  "version": 1,
  "type": "gcs",
  "interval": "12h",
  "timestamp": true,
  "sub": {
    "project_id": "$GCP_PROJECT_ID",
    "bucket": "$GCS_BUCKET",
    "path": "backup.db",
    "credentials_file": "$GOOGLE_APPLICATION_CREDENTIALS"
  }
}
```

## Usage

### Automatic Backups

Configure GCS backups when starting rqlited:

```bash
rqlited -auto-backup /path/to/gcs-backup-config.json data/
```

### Auto-Restore

Configure GCS restore with a similar config file:

```bash
rqlited -auto-restore /path/to/gcs-restore-config.json data/
```

## Features

- **Full compatibility**: Implements the same StorageClient interface as S3
- **Metadata support**: Stores backup IDs in GCS object metadata
- **Timestamped backups**: Optional timestamp prefixing for backup files
- **Compression**: Supports gzip compression of backup files
- **Environment variables**: Full support for env var expansion in config
- **Error handling**: Robust error handling and logging

## Migration from S3

To migrate from S3 to GCS, simply change:
1. `"type": "s3"` to `"type": "gcs"`
2. Replace S3-specific fields with GCS equivalents
3. Update authentication credentials

All other configuration options (interval, vacuum, compression, etc.) work identically.

## Implementation Details

- Uses Google Cloud Storage Go SDK
- Backup IDs stored in object metadata using key `x-rqlite-auto-backup-id`
- Supports bucket creation if bucket doesn't exist
- Proper resource cleanup and context handling
- Full compatibility with existing backup/restore infrastructure