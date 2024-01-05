#!/usr/bin/env python

import os
import json
import unittest
import sqlite3
import time

from helpers import Node, deprovision_node, write_random_file, random_string, env_present, gunzip_file, gzip_compress, temp_file, d_
from s3 import download_s3_object, delete_s3_object, upload_s3_object

S3_BUCKET = 'rqlite-testing-circleci'
S3_BUCKET_REGION = 'us-west-2'

RQLITED_PATH = os.environ['RQLITED_PATH']

class TestAutoRestoreS3(unittest.TestCase):
  def create_sqlite_file(self):
    tmp_file = temp_file()
    conn = sqlite3.connect(tmp_file)
    c = conn.cursor()
    c.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    c.execute('INSERT INTO foo(name) VALUES("fiona")')
    conn.commit()
    conn.close()
    return tmp_file

  @unittest.skipUnless(env_present('RQLITE_S3_ACCESS_KEY'), "S3 credentials not available")
  def test_not_compressed(self):
    '''Test that automatic restores from AWS S3 work with non-compressed data'''

    node = None
    cfg = None
    path = None

    access_key_id = os.environ['RQLITE_S3_ACCESS_KEY']
    secret_access_key_id = os.environ['RQLITE_S3_SECRET_ACCESS_KEY']

    # Upload a test SQLite file to S3.
    tmp_file = self.create_sqlite_file()

    path = "restore/"+random_string(32)
    upload_s3_object(access_key_id, secret_access_key_id, S3_BUCKET, path, tmp_file)

    # Create the auto-restore config file
    auto_restore_cfg = {
      "version": 1,
      "type": "s3",
      "sub" : {
         "access_key_id": access_key_id,
         "secret_access_key": secret_access_key_id,
         "region": S3_BUCKET_REGION,
         "bucket": S3_BUCKET,
         "path": path
      }
    }
    cfg = write_random_file(json.dumps(auto_restore_cfg))

    node = Node(RQLITED_PATH, '0', auto_restore=cfg)
    node.start()
    node.wait_for_ready()
    j = node.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    deprovision_node(node)
    os.remove(cfg)
    os.remove(tmp_file)
    delete_s3_object(access_key_id, secret_access_key_id, S3_BUCKET, path)

  @unittest.skipUnless(env_present('RQLITE_S3_ACCESS_KEY'), "S3 credentials not available")
  def test_compressed(self):
    '''Test that automatic restores from AWS S3 work with compressed data'''

    node = None
    cfg = None
    path = None

    access_key_id = os.environ['RQLITE_S3_ACCESS_KEY']
    secret_access_key_id = os.environ['RQLITE_S3_SECRET_ACCESS_KEY']

    # Upload a test SQLite file to S3.
    tmp_file = self.create_sqlite_file()
    compressed_tmp_file = temp_file()
    gzip_compress(tmp_file, compressed_tmp_file)

    path = "restore/"+random_string(32)
    upload_s3_object(access_key_id, secret_access_key_id, S3_BUCKET, path, compressed_tmp_file)

    # Create the auto-restore config file
    auto_restore_cfg = {
      "version": 1,
      "type": "s3",
      "sub" : {
         "access_key_id": access_key_id,
         "secret_access_key": secret_access_key_id,
         "region": S3_BUCKET_REGION,
         "bucket": S3_BUCKET,
         "path": path
      }
    }
    cfg = write_random_file(json.dumps(auto_restore_cfg))

    node = Node(RQLITED_PATH, '0', auto_restore=cfg)
    node.start()
    node.wait_for_ready()
    j = node.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    deprovision_node(node)
    os.remove(cfg)
    os.remove(tmp_file)
    os.remove(compressed_tmp_file)
    delete_s3_object(access_key_id, secret_access_key_id, S3_BUCKET, path)

  @unittest.skipUnless(env_present('RQLITE_S3_ACCESS_KEY'), "S3 credentials not available")
  def test_skipped_if_data(self):
    '''Test that automatic restores are skipped if the node has data'''

    node = None
    cfg = None
    path = None

    access_key_id = os.environ['RQLITE_S3_ACCESS_KEY']
    secret_access_key_id = os.environ['RQLITE_S3_SECRET_ACCESS_KEY']

    # Upload a test SQLite file to S3.
    tmp_file = self.create_sqlite_file()
    compressed_tmp_file = temp_file()
    gzip_compress(tmp_file, compressed_tmp_file)

    path = "restore/"+random_string(32)
    upload_s3_object(access_key_id, secret_access_key_id, S3_BUCKET, path, compressed_tmp_file)

    # Create the auto-restore config file
    auto_restore_cfg = {
      "version": 1,
      "type": "s3",
      "sub" : {
         "access_key_id": access_key_id,
         "secret_access_key": secret_access_key_id,
         "region": S3_BUCKET_REGION,
         "bucket": S3_BUCKET,
         "path": path
      }
    }
    cfg = write_random_file(json.dumps(auto_restore_cfg))

    # Create a new node, write some data to it.
    n0 = Node(RQLITED_PATH, '0')
    n0.start()
    n0.wait_for_ready()
    n0.execute('CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    n0.stop()

    # Create a new node, using the directory from the previous node, but check
    # that data is not restored from S3, wiping out the existing data.
    n1 = Node(RQLITED_PATH, '0', dir=n0.dir, auto_restore=cfg)
    n1.start()
    n1.wait_for_ready()
    j = n1.query('SELECT * FROM bar')
    self.assertEqual(j, d_("{'results': [{'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))
    j = n1.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'error': 'no such table: foo'}]}"))

    deprovision_node(n0)
    deprovision_node(n1)
    os.remove(cfg)
    os.remove(tmp_file)
    os.remove(compressed_tmp_file)
    delete_s3_object(access_key_id, secret_access_key_id, S3_BUCKET, path)

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
    delete_s3_object(access_key_id, secret_access_key_id, S3_BUCKET, path)

  @unittest.skipUnless(env_present('RQLITE_S3_ACCESS_KEY'), "S3 credentials not available")
  def test_no_compress_vacuum(self):
    '''Test that automatic backups to AWS S3 work with compression off and vacuum on'''
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
      "vacuum": True,
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
    delete_s3_object(access_key_id, secret_access_key_id, S3_BUCKET, path)
    
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
    for i in range(1000):
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
    c.execute('SELECT count(*) FROM foo WHERE name="fiona"')
    rows = c.fetchall()
    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0][0], 1000)
    conn.close()

    deprovision_node(node)
    os.remove(cfg)
    os.remove(compressed_backup_file)
    os.remove(backup_file)
    delete_s3_object(access_key_id, secret_access_key_id, S3_BUCKET, path)


if __name__ == "__main__":
  unittest.main(verbosity=2)
