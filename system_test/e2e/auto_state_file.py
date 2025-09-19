#!/usr/bin/env python

import os
import json
import unittest
import sqlite3
import time
import tempfile
import shutil
import glob
import gzip

from helpers import Node, deprovision_node, write_random_file, random_string, d_, Cluster

RQLITED_PATH = os.environ['RQLITED_PATH']

class TestAutoBackup_File(unittest.TestCase):
  def test_backup_timestamp_true(self):
    '''Test automatic backup to file with timestamp=true'''
    node = None
    backup_dir = None
    
    try:
      # Create a temporary directory for backups
      backup_dir = tempfile.mkdtemp()
      
      # Create the auto-backup config file with timestamp enabled
      auto_backup_cfg = {
        "version": 1,
        "type": "file",
        "interval": "1s",
        "timestamp": True,
        "no_compress": True,
        "vacuum": False,
        "sub": {
          "dir": backup_dir,
          "name": "backup.sqlite"
        }
      }
      auto_backup_cfg_file = write_random_file(json.dumps(auto_backup_cfg))
      
      # Create a node, enable automatic backups, and start it
      node = Node(RQLITED_PATH, '0', auto_backup=auto_backup_cfg_file)
      node.start()
      node.wait_for_leader()
      
      # Create a table and insert data
      node.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
      node.execute('INSERT INTO foo(name) VALUES("alice")')
      
      # Wait for first backup
      node.wait_for_upload(1)
      node.wait_until_uploads_idle()
      
      # Insert more data to trigger second backup
      node.execute('INSERT INTO foo(name) VALUES("bob")')
      node.wait_for_upload(2)
      node.wait_until_uploads_idle()
      
      # Check that multiple timestamped backup files exist
      backup_files = glob.glob(os.path.join(backup_dir, "*_backup.sqlite"))
      self.assertGreaterEqual(len(backup_files), 2, f"Expected at least 2 backup files, found: {backup_files}")
      
      # Verify the latest backup contains the expected data
      latest_backup = max(backup_files, key=os.path.getctime)
      conn = sqlite3.connect(latest_backup)
      cursor = conn.cursor()
      cursor.execute("SELECT * FROM foo ORDER BY id")
      rows = cursor.fetchall()
      conn.close()
      self.assertEqual(len(rows), 2)
      self.assertEqual(rows[0], (1, 'alice'))
      self.assertEqual(rows[1], (2, 'bob'))

      # Verify the oldest backup contains only the first entry
      oldest_backup = min(backup_files, key=os.path.getctime)
      conn = sqlite3.connect(oldest_backup)
      cursor = conn.cursor()
      cursor.execute("SELECT * FROM foo ORDER BY id")
      rows = cursor.fetchall()
      conn.close()
      self.assertEqual(len(rows), 1)
      self.assertEqual(rows[0], (1, 'alice'))
      
    finally:
      if node:
        deprovision_node(node)
      if 'auto_backup_cfg_file' in locals():
        os.remove(auto_backup_cfg_file)
      if backup_dir and os.path.exists(backup_dir):
        shutil.rmtree(backup_dir)

  def test_backup_timestamp_false(self):
    '''Test automatic backup to file with timestamp=false (overwrite mode)'''
    node = None
    backup_dir = None
    
    try:
      # Create a temporary directory for backups
      backup_dir = tempfile.mkdtemp()
      backup_file = os.path.join(backup_dir, "backup.sqlite")
      
      # Create the auto-backup config file with timestamp disabled
      auto_backup_cfg = {
        "version": 1,
        "type": "file",
        "interval": "1s",
        "timestamp": False,
        "no_compress": True,
        "vacuum": False,
        "sub": {
          "dir": backup_dir,
          "name": "backup.sqlite"
        }
      }
      auto_backup_cfg_file = write_random_file(json.dumps(auto_backup_cfg))
      
      # Create a node, enable automatic backups, and start it
      node = Node(RQLITED_PATH, '0', auto_backup=auto_backup_cfg_file)
      node.start()
      node.wait_for_leader()
      
      # Create a table and insert data
      node.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
      node.execute('INSERT INTO foo(name) VALUES("alice")')
      
      # Wait for first backup
      node.wait_for_upload(1)
      node.wait_until_uploads_idle()
      
      # Check that backup file exists
      self.assertTrue(os.path.exists(backup_file), "Backup file should exist")
      
      # Record the initial modification time
      initial_mtime = os.path.getmtime(backup_file)
      
      # Insert more data to trigger second backup (should overwrite)
      node.execute('INSERT INTO foo(name) VALUES("bob")')
      node.wait_for_upload(2)
      node.wait_until_uploads_idle()
      
      # Check that only one backup file exists (overwrite mode)
      backup_files = glob.glob(os.path.join(backup_dir, "*.sqlite"))
      timestamped_files = [f for f in backup_files if "_backup.sqlite" in f]
      self.assertEqual(len(timestamped_files), 0, "Should have no timestamped files")
      self.assertEqual(len(backup_files), 1, f"Should have exactly 1 backup file, found: {backup_files}")
      self.assertEqual(backup_files[0], backup_file)
      
      # Verify the file was overwritten (modification time changed)
      final_mtime = os.path.getmtime(backup_file)
      self.assertGreater(final_mtime, initial_mtime, "Backup file should have been overwritten")
      
      # Verify the backup contains all the data
      conn = sqlite3.connect(backup_file)
      cursor = conn.cursor()
      cursor.execute("SELECT * FROM foo ORDER BY id")
      rows = cursor.fetchall()
      conn.close()
      
      self.assertEqual(len(rows), 2)
      self.assertEqual(rows[0], (1, 'alice'))
      self.assertEqual(rows[1], (2, 'bob'))
      
    finally:
      if node:
        deprovision_node(node)
      if 'auto_backup_cfg_file' in locals():
        os.remove(auto_backup_cfg_file)
      if backup_dir and os.path.exists(backup_dir):
        shutil.rmtree(backup_dir)

  def test_backup_with_compression(self):
    '''Test automatic backup to file with compression enabled'''
    node = None
    backup_dir = None
    
    try:
      # Create a temporary directory for backups
      backup_dir = tempfile.mkdtemp()
      
      # Create the auto-backup config file with compression
      auto_backup_cfg = {
        "version": 1,
        "type": "file",
        "interval": "1s",
        "timestamp": True,
        "no_compress": False,  # Enable compression
        "vacuum": False,
        "sub": {
          "dir": backup_dir,
          "name": "backup.sqlite"
        }
      }
      auto_backup_cfg_file = write_random_file(json.dumps(auto_backup_cfg))
      
      # Create a node, enable automatic backups, and start it
      node = Node(RQLITED_PATH, '0', auto_backup=auto_backup_cfg_file)
      node.start()
      node.wait_for_leader()
      
      # Create a table and insert data
      node.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
      node.execute('INSERT INTO foo(name) VALUES("alice")')
      
      # Wait for backup
      node.wait_for_upload(1)
      node.wait_until_uploads_idle()
      
      # Check that backup files exist
      backup_files = glob.glob(os.path.join(backup_dir, "*_backup.sqlite"))
      self.assertGreaterEqual(len(backup_files), 1, f"Expected at least 1 backup file, found: {backup_files}")
      
      # With compression enabled, the files should be compressed
      # We should check if files are compressed by trying to uncompress them
      latest_backup = max(backup_files, key=os.path.getctime)
      
      # Try to read as gzipped file first
      try:
        with gzip.open(latest_backup, 'rb') as f:
          # If we can read it as gzip, decompress to temp file and test
          uncompressed_data = f.read()
          
        # Write uncompressed data to a temporary file
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as temp_file:
          temp_file.write(uncompressed_data)
          temp_sqlite_path = temp_file.name
          
        try:
          # Test the uncompressed SQLite data
          conn = sqlite3.connect(temp_sqlite_path)
          cursor = conn.cursor()
          cursor.execute("SELECT * FROM foo")
          rows = cursor.fetchall()
          conn.close()
          
          self.assertEqual(len(rows), 1)
          self.assertEqual(rows[0], (1, 'alice'))
        finally:
          os.unlink(temp_sqlite_path)
          
      except (gzip.BadGzipFile, OSError):
        self.fail("Backup file is not properly compressed")
      
    finally:
      if node:
        deprovision_node(node)
      if 'auto_backup_cfg_file' in locals():
        os.remove(auto_backup_cfg_file)
      if backup_dir and os.path.exists(backup_dir):
        shutil.rmtree(backup_dir)

if __name__ == '__main__':
  unittest.main(verbosity=2)