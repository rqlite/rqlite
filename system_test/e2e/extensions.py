#!/usr/bin/env python
#
# End-to-end testing using actual rqlited binary.
#
# To run a specific test, execute
#
#  python system_test/full_system_test.py Class.test

import os
import unittest
import tempfile
import shutil

from helpers import Node, Cluster, d_

RQLITED_PATH = os.environ['RQLITED_PATH']
EXTENSIONS_PATH = os.environ['EXTENSIONS_PATH']
EXTENSIONS_PATH_DIR = os.environ['EXTENSIONS_PATH_DIR']
EXTENSIONS_PATH_ZIP = os.environ['EXTENSIONS_PATH_ZIP']
EXTENSIONS_PATH_TAR_GZIP = os.environ['EXTENSIONS_PATH_TAR_GZIP']
EXTENSIONS_PATH_MULTIPLE = os.environ['EXTENSIONS_PATH_MULTIPLE']

class TestExtensions_File(unittest.TestCase):
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

class TestExtensions_Dir(unittest.TestCase):
  def setUp(self):
    n0 = Node(RQLITED_PATH, '0', extensions_path=EXTENSIONS_PATH_DIR)
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

class TestExtensions_TarGzipped(unittest.TestCase):
  def setUp(self):
    n0 = Node(RQLITED_PATH, '0', extensions_path=EXTENSIONS_PATH_TAR_GZIP)
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

class TestExtensions_Multiple(unittest.TestCase):
  def setUp(self):
    n0 = Node(RQLITED_PATH, '0', extensions_path=EXTENSIONS_PATH_MULTIPLE)
    n0.start()
    n0.wait_for_leader()
    self.cluster = Cluster([n0])

  def tearDown(self):
    self.cluster.deprovision()

  def test(self):
    n = self.cluster.wait_for_leader()
    loaded = n.extensions()
    if 'rot13.so' not in loaded:
      self.fail('rot13 not loaded')
    if 'carray.so' not in loaded:
      self.fail('carray not loaded')

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

class TestExtensions_NotLoaded_EmptyDir(unittest.TestCase):
  '''Test that an empty extensions directory does not rqlited to fail to start'''
  def setUp(self):
    self.test_dir = tempfile.mkdtemp()
    n0 = Node(RQLITED_PATH, '0', extensions_path=self.test_dir)
    n0.start()
    n0.wait_for_leader()
    self.cluster = Cluster([n0])

  def tearDown(self):
    self.cluster.deprovision()
    if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

  def test_rot13(self):
    n = self.cluster.wait_for_leader()
    j = n.query('SELECT rot13("hello")')
    expected = d_('{"results": [{"error": "no such function: rot13"}]}')
    self.assertEqual(j, expected)

if __name__ == "__main__":
  unittest.main(verbosity=2)
