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
EXTENSIONS_PATH_MISC = os.environ['EXTENSIONS_PATH_MISC']

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

class TestExtensions_File_Reload(unittest.TestCase):
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

    # Now, get a backup from the database so we can check that various restores always
    # reload the extension.
    fd, db_file = tempfile.mkstemp()
    os.close(fd)
    n.backup(db_file)

    n.boot(db_file)
    j = n.query('SELECT rot13("hello")')
    expected = d_('{"results": [{"columns": ["rot13(\\"hello\\")"], "types": ["text"], "values": [["uryyb"]]}]}')
    self.assertEqual(j, expected)

    n.restore(db_file)
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

class TestExtensions_MiscExtensions(unittest.TestCase):
  def setUp(self):
    n0 = Node(RQLITED_PATH, '0', extensions_path=EXTENSIONS_PATH_MISC)
    n0.start()
    n0.wait_for_leader()
    self.cluster = Cluster([n0])

  def tearDown(self):
    self.cluster.deprovision()

  def test(self):
    '''Test that simple queries work as expected, ensuring that the presence of misc extensions does not break core functionality'''
    n = self.cluster.wait_for_leader()
    n.status()

    if len(n.extensions()) == 0:
      self.fail('extensions not loaded')

    j = n.execute('CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))

    j = n.execute('INSERT INTO bar(name) VALUES("fiona")')
    applied = n.wait_for_all_applied()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = n.query('SELECT * from bar')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    j = n.execute('INSERT INTO bar(name) VALUES("declan")')
    applied = n.wait_for_all_applied()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 2, 'rows_affected': 1}]}"))
    j = n.query('SELECT * from bar where id=2')
    self.assertEqual(j, d_("{'results': [{'values': [[2, 'declan']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    j = n.query('SELECT * from bar', text=True)
    self.assertEqual(str(j), '{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"declan"]]}]}')
    j = n.query('SELECT * from bar where name="non-existent"', text=True)
    self.assertEqual(str(j), '{"results":[{"columns":["id","name"],"types":["integer","text"]}]}')

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
