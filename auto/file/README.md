# File Storage Backend for Auto-Backup

The file storage backend allows rqlite to perform automatic backups to the local filesystem. This provides a simple backup solution for on-premises deployments or test environments.

## Configuration

The file backend supports all the same configuration options as S3 and GCS backends:

### Basic Configuration

```json
{
  "version": 1,
  "type": "file",
  "interval": "30s",
  "timestamp": false,
  "no_compress": false,
  "vacuum": true,
  "sub": {
    "dir": "/var/backups/rqlite",
    "file": "backup.sqlite"
  }
}
```

### Configuration Options

- `dir` (required): Target directory for backup files. Directory will be created if it doesn't exist.
- `file` (required): Base filename for backups. Must be a simple filename without path separators for security.
- `timestamp`: Controls backup behavior:
  - `false` (default): Overwrites the same file on each backup
  - `true`: Creates timestamped files (e.g., `20250919143045.123_backup.sqlite`)
- `no_compress`: When `false`, backup files are gzipped
- `vacuum`: When `true`, runs VACUUM before backup to optimize database size
- `interval`: How frequently to perform backups

### Timestamped Mode Example

```json
{
  "version": 1,
  "type": "file",
  "interval": "1h",
  "timestamp": true,
  "no_compress": true,
  "vacuum": false,
  "sub": {
    "dir": "/var/backups/rqlite",
    "file": "backup.sqlite"
  }
}
```

This configuration will create files like:
- `20250919143045.123_backup.sqlite`
- `20250919153045.456_backup.sqlite`
- etc.

## Usage

1. Create a configuration file (e.g., `backup_config.json`) with your settings
2. Start rqlited with auto-backup enabled:
   ```bash
   rqlited -auto-backup backup_config.json data/
   ```

## Security Notes

- The `file` parameter is validated to prevent directory traversal attacks
- Only simple filenames are allowed (no path separators like `/` or `../`)
- The backup directory must be writable by the rqlite process

## File Management

- **Overwrite mode** (`timestamp: false`): Only one backup file exists at a time
- **Timestamped mode** (`timestamp: true`): Multiple backup files accumulate over time
- **Retention**: The file backend does not automatically delete old backups in timestamped mode - implement your own retention policy using external tools like logrotate or cron

## Atomic Operations

All backup operations are atomic:
1. Data is written to a temporary file in the same directory
2. The temporary file is synced to disk
3. The temporary file is atomically renamed to the final location
4. Metadata is updated only after successful file creation

This ensures that partial backup files are never left behind and metadata always points to complete, valid backups.