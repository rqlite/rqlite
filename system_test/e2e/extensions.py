#!/usr/bin/env python
#
# End-to-end testing using actual rqlited binary.
#
# To run a specific test, execute
#
#  python system_test/full_system_test.py Class.test

import os
import unittest

from helpers import Node, Cluster, d_, env_present, write_random_file
from s3 import download_s3_object

S3_BUCKET = 'rqlite-testing-circleci-extensions'

RQLITED_PATH = os.environ['RQLITED_PATH']
EXTENSIONS_PATH = os.environ['EXTENSIONS_PATH']
EXTENSIONS_PATH_ZIP = os.environ['EXTENSIONS_PATH_ZIP']

class TestExtensions(unittest.TestCase):
  def setUp(self):
    n0 = Node(RQLITED_PATH, '0', extensions_path=EXTENSIONS_PATH)
    n0.start()
    n0.wait_for_leader()
    self.cluster = Cluster([n0])

  def tearDown(self):
    self.cluster.deprovision()

  def test_rot13(self):
    n = self.cluster.wait_for_leader()
    j = n.query('SELECT rot13("hello")')
    expected = d_('{"results": [{"columns": ["rot13(\\"hello\\")"], "types": ["text"], "values": [["uryyb"]]}]}')
    self.assertEqual(j, expected)

class TestExtensions_Zipped(unittest.TestCase):
  def setUp(self):
    n0 = Node(RQLITED_PATH, '0', extensions_path=EXTENSIONS_PATH_ZIP)
    n0.start()
    n0.wait_for_leader()
    self.cluster = Cluster([n0])

  def tearDown(self):
    self.cluster.deprovision()

  def test_rot13(self):
    n = self.cluster.wait_for_leader()
    j = n.query('SELECT rot13("hello")')
    expected = d_('{"results": [{"columns": ["rot13(\\"hello\\")"], "types": ["text"], "values": [["uryyb"]]}]}')
    self.assertEqual(j, expected)

@unittest.skipUnless(env_present('RQLITE_S3_ACCESS_KEY'), "S3 credentials not available")
class TestExtensions_sqlean(unittest.TestCase):
  def tearDown(self):
    self.cluster.deprovision()

  def test(self):
    access_key_id = os.environ['RQLITE_S3_ACCESS_KEY']
    secret_access_key_id = os.environ['RQLITE_S3_SECRET_ACCESS_KEY']
    extensions = download_s3_object(access_key_id, secret_access_key_id, S3_BUCKET, 'sqlean-linux-x86.zip')
    extensions_file = write_random_file(extensions, mode='wb')

    n0 = Node(RQLITED_PATH, '0', extensions_path=extensions_file)
    n0.start()
    n0.wait_for_leader()
    self.cluster = Cluster([n0])

class TestExtensions_NotLoaded(unittest.TestCase):
  def setUp(self):
    n0 = Node(RQLITED_PATH, '0')
    n0.start()
    n0.wait_for_leader()
    self.cluster = Cluster([n0])

  def tearDown(self):
    self.cluster.deprovision()

  def test_rot13(self):
    n = self.cluster.wait_for_leader()
    j = n.query('SELECT rot13("hello")')
    expected = d_('{"results": [{"error": "no such function: rot13"}]}')
    self.assertEqual(j, expected)

if __name__ == "__main__":
  unittest.main(verbosity=2)
