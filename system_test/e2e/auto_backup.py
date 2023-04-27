#!/usr/bin/env python
#
# End-to-end testing using actual rqlited binary.
#
# To run a specific test, execute
#
#  python system_test/full_system_test.py Class.test

import boto3
import os
import json
import unittest
import random
import sqlite3
import string
import time

from helpers import Node, deprovision_node, write_random_file

RQLITED_PATH = os.environ['RQLITED_PATH']

def random_string(N):
  return ''.join(random.choices(string.ascii_uppercase + string.digits, k=N))

def delete_s3_object(bucket_name, object_key):
    """
    Delete an object from an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        object_key (str): The key of the object to delete.
    """
    # Create a boto3 client for S3
    s3_client = boto3.client('s3')

    # Delete the object from the S3 bucket
    s3_client.delete_object(Bucket=bucket_name, Key=object_key)

def download_s3_object(bucket_name, object_key):
    """
    Download an object from an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        object_key (str): The key of the object to download.
    """
    # Create a boto3 client for S3
    s3_client = boto3.client('s3')

    # Download the object from the S3 bucket
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)

    # Return the object contents
    return response['Body'].read()

class TestAutoBackupS3(unittest.TestCase):
  '''Test that automatic backups to AWS S3 work'''
  def test(self):
    try:
      if os.environ['RQLITE_S3_ACCESS_KEY'] == "":
        return
    except KeyError:
      return

    # Create the auto-backup config file
    path = random_string(32)
    auto_backup_cfg = {
      "version": 1,
      "type": "s3",
      "interval": "5s",
      "sub" : {
         "access_key_id": os.environ['RQLITE_S3_ACCESS_KEY'],
         "secret_access_key": os.environ['RQLITE_S3_SECRET_ACCESS_KEY'],
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
    os.environ['AWS_ACCESS_KEY_ID'] = os.environ['RQLITE_S3_ACCESS_KEY']
    os.environ['AWS_SECRET_ACCESS_KEY'] = os.environ['RQLITE_S3_SECRET_ACCESS_KEY']

    backupData = download_s3_object('rqlite-testing-circleci', path)
    backupFile = write_random_file(backupData, mode='wb')
    conn = sqlite3.connect(backupFile)
    c = conn.cursor()
    c.execute('SELECT * FROM foo')
    rows = c.fetchall()
    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0][1], 'fidona')
    conn.close()

    # Remove the backup file and S3 object
    os.remove(backupFile)
    delete_s3_object('rqlite-testing-circleci', path)


if __name__ == "__main__":
  unittest.main(verbosity=2)