import sqlite3
import shutil
import os

# Database file
db_file = 'mydatabase.db'

# Open a connection to SQLite database
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Enable WAL mode and disable automatic checkpointing
cursor.execute("PRAGMA journal_mode=WAL;")
cursor.execute("PRAGMA wal_autocheckpoint=0;")
cursor.execute("CREATE TABLE foo (id INTEGER PRIMARY KEY, value TEXT);")
conn.commit()

# Checkpoint the WAL file so we've got just a SQLite file
conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
shutil.copy(db_file, 'backup.db')

for i in range(0, 4):
    # Write a new row
    cursor.execute(f"INSERT INTO foo (value) VALUES ('Row {i}');")
    conn.commit()

    # Copy the newly-created WAL
    shutil.copy(db_file + '-wal', f'wal-{i:02}')

    # Checkpoint the WAL file
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    conn.commit()

conn.close()
