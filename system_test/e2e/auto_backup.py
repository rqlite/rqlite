#!/usr/bin/env python
#
# End-to-end testing using actual rqlited binary.
#
# To run a specific test, execute
#
#  python system_test/full_system_test.py Class.test

import os
import unittest
import random
import string
import time

from helpers import Node, deprovision_node, write_random_file

RQLITED_PATH = os.environ['RQLITED_PATH']

def random_string(N):
  return ''.join(random.choices(string.ascii_uppercase + string.digits, k=N))

class TestAutoBackupS3(unittest.TestCase):
  '''Test that automatic backups to AWS S3 work'''
  def test(self):
    try:
      if os.environ['RQLITE_S3_ACCESS_KEY'] == "":
        return
    except KeyError:
      return

    path = random_string(32)
    auto_backup_cfg = {
      "version": 1,
      "type": "s3",
      "interval": "5s",
      "sub" : {
         "access_key_id": os.environ['RQLITE_S3_ACCESS_KEY'],
         "secret_access_key": os.environ['RQLITE_S3_SECRET_KEY'],
         "region": "us-west-2",
         "bucket": "rqlite-testing-circleci",
         "path": "%s/db.sqlite3" % path
      }
    }

    cfg = write_random_file(str(auto_backup_cfg))

    node = Node(RQLITED_PATH, '0', auto_backup=cfg)
    node.start()
    node.wait_for_leader()
    node.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    node.execute('INSERT INTO foo(name) VALUES("fiona")')
    node.wait_for_all_fsm()

    time.Sleep(10)

    deprovision_node(node)
    os.remove(cfg)


if __name__ == "__main__":
  unittest.main(verbosity=2)