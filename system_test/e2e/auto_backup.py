#!/usr/bin/env python
#
# End-to-end testing using actual rqlited binary.
#
# To run a specific test, execute
#
#  python system_test/full_system_test.py Class.test

import os
import json
import unittest
import sqlite3
import time

from helpers import Node, deprovision_node, write_random_file, random_string, env_present, gunzip_file
from s3 import download_s3_object, delete_s3_object

S3_BUCKET = 'rqlite-testing-circleci'
S3_BUCKET_REGION = 'us-west-2'

RQLITED_PATH = os.environ['RQLITED_PATH']

class TestAutoBackupS3(unittest.TestCase):
  @unittest.skipUnless(env_present('RQLITE_S3_ACCESS_KEY'), "S3 credentials not available")
  def test_no_compress(self):
    '''Test that automatic backups to AWS S3 work with compression off'''
    node = None
    cfg = None
    path = None
    backup_file = None

    access_key_id = os.environ['RQLITE_S3_ACCESS_KEY']
    secret_access_key_id = os.environ['RQLITE_S3_SECRET_ACCESS_KEY']

    # Create the auto-backup config file
    path = random_string(32)
    auto_backup_cfg = {
      "version": 1,
      "type": "s3",
      "interval": "1s",
      "no_compress": True,
      "sub" : {
         "access_key_id": access_key_id,
         "secret_access_key": secret_access_key_id,
         "region": S3_BUCKET_REGION,
         "bucket": S3_BUCKET,
         "path": path
      }
    }
    cfg = write_random_file(json.dumps(auto_backup_cfg))

    # Create a node, enable automatic backups, and start it. Then
    # create a table and insert a row. Wait for a backup to happen.
    node = Node(RQLITED_PATH, '0', auto_backup=cfg)
    node.start()
    node.wait_for_leader()
    node.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    node.execute('INSERT INTO foo(name) VALUES("fiona")')
    node.wait_for_all_fsm()
    time.sleep(5)

    # Download the backup file from S3 and check it.
    backup_data = download_s3_object(access_key_id, secret_access_key_id,
                                     S3_BUCKET, path)
    backup_file = write_random_file(backup_data, mode='wb')
    conn = sqlite3.connect(backup_file)
    c = conn.cursor()
    c.execute('SELECT * FROM foo')
    rows = c.fetchall()
    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0][1], 'fiona')
    conn.close()

    deprovision_node(node)
    os.remove(cfg)
    os.remove(backup_file)
    delete_s3_object(access_key_id, secret_access_key_id,
                     S3_BUCKET, path)
    
  @unittest.skipUnless(env_present('RQLITE_S3_ACCESS_KEY'), "S3 credentials not available")
  def test_compress(self):
    '''Test that automatic backups to AWS S3 work with compression on'''
    node = None
    cfg = None
    path = None
    compressed_backup_file = None
    backup_file = None

    access_key_id = os.environ['RQLITE_S3_ACCESS_KEY']
    secret_access_key_id = os.environ['RQLITE_S3_SECRET_ACCESS_KEY']

    # Create the auto-backup config file
    path = random_string(32)
    auto_backup_cfg = {
      "version": 1,
      "type": "s3",
      "interval": "1s",
      "sub" : {
         "access_key_id": access_key_id,
         "secret_access_key": secret_access_key_id,
         "region": S3_BUCKET_REGION,
         "bucket": S3_BUCKET,
         "path": path
      }
    }
    cfg = write_random_file(json.dumps(auto_backup_cfg))

    # Create a node, enable automatic backups, and start it. Then
    # create a table and insert a row. Wait for a backup to happen.
    node = Node(RQLITED_PATH, '0', auto_backup=cfg)
    node.start()
    node.wait_for_leader()
    node.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    node.execute('INSERT INTO foo(name) VALUES("fiona")')
    node.wait_for_all_fsm()
    time.sleep(5)

    # Download the backup file from S3 and check it.
    backup_data = download_s3_object(access_key_id, secret_access_key_id,
                                     S3_BUCKET, path)
    compressed_backup_file = write_random_file(backup_data, mode='wb')
    backup_file = gunzip_file(compressed_backup_file)
    conn = sqlite3.connect(backup_file)
    c = conn.cursor()
    c.execute('SELECT * FROM foo')
    rows = c.fetchall()
    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0][1], 'fiona')
    conn.close()

    deprovision_node(node)
    os.remove(cfg)
    os.remove(compressed_backup_file)
    os.remove(backup_file)
    delete_s3_object(access_key_id, secret_access_key_id,
                     S3_BUCKET, path)


if __name__ == "__main__":
  unittest.main(verbosity=2)
