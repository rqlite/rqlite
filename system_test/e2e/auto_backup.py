#!/usr/bin/env python
#
# End-to-end testing using actual rqlited binary.
#
# To run a specific test, execute
#
#  python system_test/full_system_test.py Class.test

import os
import unittest

from helpers import Node, deprovision_node

RQLITED_PATH = os.environ['RQLITED_PATH']

class TestAutoBackupS3(unittest.TestCase):
  '''Test that automatic backups to AWS S3 work'''
  def test(self):
    if os.environ['RQLITE_S3_ACCESS_KEY'] is not None and os.environ['RQLITE_S3_SECRET_KEY'] is not None:
      print("Running test because environment variables are set")
    else:
      print("Skipping test because environment variables are not set")
      return

    n0 = Node(RQLITED_PATH, '0')
    n0.start()
    n0.wait_until_leader()
    deprovision_node(n0)


if __name__ == "__main__":
  unittest.main(verbosity=2)