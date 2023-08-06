import sqlite3
import shutil
import os

# Database file
db_file = 'mydatabase.db'

# Open a connection to SQLite database
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Enable WAL mode
cursor.execute("PRAGMA journal_mode=WAL;")
# Disable automatic checkpoint
cursor.execute("PRAGMA wal_autocheckpoint=0;")

cursor.execute("CREATE TABLE foo (id INTEGER PRIMARY KEY, value TEXT);")
conn.commit()

# Copy the SQLite file and WAL file
shutil.copy(db_file, 'backup.db')
shutil.copy(db_file + '-wal', 'wal-01')

# Checkpoint the original WAL file
conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")

# Loop for the desired number of iterations
for i in range(0, 4):
    # Write a new row
    cursor.execute(f"INSERT INTO foo (value) VALUES ('Row {i}');")
    conn.commit()

    # Copy the newly-created WAL
    shutil.copy(db_file + '-wal', f'wal-{i:02}')

    # Checkpoint the WAL file
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")

conn.close()
