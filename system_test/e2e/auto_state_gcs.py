#!/usr/bin/env python

import os
import json
import unittest
import sqlite3

from helpers import Node, deprovision_node, write_random_file, random_string, env_present
from gcs import delete_gcs_object, download_gcs_object

GCS_BUCKET = 'rqlite-testing-circleci'

RQLITED_PATH = os.environ['RQLITED_PATH']

class TestAutoBackup_GCS(unittest.TestCase):
  @unittest.skipUnless(env_present('RQLITE_GCS_CREDENTIALS'), "GCS credentials not available")
  def test_no_compress(self):
    '''Test that automatic backups to Google Cloud Storage work with compression off, and don't happen unnecessarily'''
    node = None
    cfg = None
    name = random_string(32)

    project_id = os.environ['RQLITE_GCS_PROJECT_ID']
    gcs_credentials_file = write_random_file(os.environ['RQLITE_GCS_CREDENTIALS'])

    # Create the auto-backup config file
    auto_backup_cfg = {
      "version": 1,
      "type": "gcs",
      "interval": "1s",
      "no_compress": True,
      "sub" : {
        "project_id": project_id,
        "bucket": GCS_BUCKET,
        "name": name,
        "credentials_path": gcs_credentials_file

      }
    }
    cfg = write_random_file(json.dumps(auto_backup_cfg))

    # Create a node, enable automatic backups, and start it. Then
    # create a table and insert a row.
    node = Node(RQLITED_PATH, '0', auto_backup=cfg)
    node.start()
    node.wait_for_leader()
    node.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    node.wait_for_upload(1)

    # Wait and check that no further backups have been made.
    node.wait_until_uploads_idle()

    # Write one more row, confirm another backup is made.
    node.execute('INSERT INTO foo(name) VALUES("fiona")')
    node.wait_for_upload(2)

    # Wait and check that no further backups have been made.
    node.wait_until_uploads_idle()

    # Download the backup file from S3 and check it.
    backup_data = download_gcs_object(gcs_credentials_file, GCS_BUCKET, name)
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
    delete_gcs_object(gcs_credentials_file, GCS_BUCKET, name)
