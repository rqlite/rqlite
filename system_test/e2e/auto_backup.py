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

from helpers import Node, deprovision_node, write_random_file, random_string, env_present
from s3 import download_s3_object, delete_s3_object

RQLITED_PATH = os.environ['RQLITED_PATH']

class TestAutoBackupS3(unittest.TestCase):
  @unittest.skipIf(not env_present('RQLITE_S3_ACCESS_KEY'), "S3 credentials not available")
  def test(self):
    '''Test that automatic backups to AWS S3 work'''
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
         "region": "us-west-2",
         "bucket": "rqlite-testing-circleci",
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
    time.sleep(10)

    deprovision_node(node)
    os.remove(cfg)

    # Download the backup file from S3 and check it.
    backupData = download_s3_object(access_key_id, secret_access_key_id, 'rqlite-testing-circleci', path)
    backupFile = write_random_file(backupData, mode='wb')
    conn = sqlite3.connect(backupFile)
    c = conn.cursor()
    c.execute('SELECT * FROM foo')
    rows = c.fetchall()
    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0][1], 'fiona')
    conn.close()

    # Remove the backup file and S3 object
    os.remove(backupFile)
    delete_s3_object(access_key_id, secret_access_key_id, 'rqlite-testing-circleci', path)


if __name__ == "__main__":
  unittest.main(verbosity=2)