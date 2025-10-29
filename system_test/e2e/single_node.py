#!/usr/bin/env python
#
# End-to-end testing using actual rqlited binary.
#
# To run a specific test, execute
#
#  python system_test/full_system_test.py Class.test

import json
import os
import time
import unittest

from helpers import Node, Cluster, d_, deprovision_node, poll_query

RQLITED_PATH = os.environ['RQLITED_PATH']

class TestSingleNode(unittest.TestCase):
  def setUp(self):
    n0 = Node(RQLITED_PATH, '0',  raft_snap_threshold=2, raft_snap_int="1s")
    n0.start()
    n0.wait_for_leader()
    self.cluster = Cluster([n0])

  def tearDown(self):
    self.cluster.deprovision()

  def test_pragmas(self):
    '''Test that the critical configuration is correct'''
    n = self.cluster.wait_for_leader()
    ro_pragmas = n.pragmas()['ro']
    rw_pragmas = n.pragmas()['rw']
    self.assertEqual(ro_pragmas, d_("{'busy_timeout': '5000', 'foreign_keys': '0', 'journal_mode': 'wal', 'synchronous': '0', 'wal_autocheckpoint': '1000'}"))
    self.assertEqual(rw_pragmas, d_("{'busy_timeout': '5000', 'foreign_keys': '0', 'journal_mode': 'wal', 'synchronous': '0', 'wal_autocheckpoint': '0'}"))

  def test_simple_raw_queries(self):
    '''Test simple queries work as expected'''
    n = self.cluster.wait_for_leader()
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

    # Ensure raw response from API is as expected.
    j = n.query('SELECT * from bar', text=True)
    self.assertEqual(str(j), '{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"declan"]]}]}')
    j = n.query('SELECT * from bar where name="non-existent"', text=True)
    self.assertEqual(str(j), '{"results":[{"columns":["id","name"],"types":["integer","text"]}]}')

    # Ensure raw associative response from API is as expected.
    j = n.query('SELECT * from bar', text=True, associative=True)
    self.assertEqual(str(j), '{"results":[{"types":{"id":"integer","name":"text"},"rows":[{"id":1,"name":"fiona"},{"id":2,"name":"declan"}]}]}')


  def test_simple_raw_queries_unicode(self):
    '''Test simple queries with unicode work'''
    n = self.cluster.wait_for_leader()
    j = n.execute('CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT NOT NULL)')
    self.assertEqual(j, d_("{'results': [{}]}"))

    j = n.execute('INSERT INTO foo(name) VALUES ("こんにちは")')
    applied = n.wait_for_all_applied()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = n.query('SELECT * from foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'こんにちは']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))
    j = n.query('SELECT * from foo where name="こんにちは"')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'こんにちは']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))
    j = n.query('SELECT * from foo where name="こん1にちは"')
    self.assertEqual(j, d_("{'results': [{'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

  def test_simple_raw_queries_pretty(self):
    '''Test simple queries, requesting pretty output, work as expected'''
    n = self.cluster.wait_for_leader()
    j = n.execute('CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = n.execute('INSERT INTO bar(name) VALUES("fiona")')
    applied = n.wait_for_all_applied()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = n.query('SELECT * from bar', pretty=True, text=True)
    exp = '''{
    "results": [
        {
            "columns": [
                "id",
                "name"
            ],
            "types": [
                "integer",
                "text"
            ],
            "values": [
                [
                    1,
                    "fiona"
                ]
            ]
        }
    ]
}'''
    self.assertEqual(str(j), exp)

  def test_simple_parameterized_queries(self):
    '''Test parameterized queries work as expected'''
    n = self.cluster.wait_for_leader()
    j = n.execute('CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)')
    self.assertEqual(j, d_("{'results': [{}]}"))

    j = n.execute('INSERT INTO bar(name, age) VALUES(?,?)', params=["fiona", 20])
    applied = n.wait_for_all_applied()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))

    j = n.execute('INSERT INTO bar(name, age) VALUES(?,?)', params=["declan", None])
    applied = n.wait_for_all_applied()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 2, 'rows_affected': 1}]}"))

    j = n.query('SELECT * from bar WHERE age=?', params=[20])
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona', 20]], 'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))
    j = n.query('SELECT * from bar WHERE age=?', params=[21])
    self.assertEqual(j, d_("{'results': [{'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))
    j = n.query('SELECT * from bar WHERE name=?', params=['declan'])
    self.assertEqual(j, d_("{'results': [{'values': [[2, 'declan', None]], 'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))

  def test_simple_named_parameterized_queries(self):
    '''Test named parameterized queries work as expected'''
    n = self.cluster.wait_for_leader()
    j = n.execute('CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)')
    self.assertEqual(j, d_("{'results': [{}]}"))

    j = n.execute('INSERT INTO bar(name, age) VALUES(?,?)', params=["fiona", 20])
    applied = n.wait_for_all_applied()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))

    j = n.query('SELECT * from bar')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona', 20]], 'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))

    j = n.query('SELECT * from bar WHERE age=:age', params={"age": 20})
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona', 20]], 'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))

    j = n.execute('INSERT INTO bar(name, age) VALUES(?,?)', params=["declan", None])
    applied = n.wait_for_all_applied()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 2, 'rows_affected': 1}]}"))

    j = n.query('SELECT * from bar WHERE name=:name', params={"name": "declan"})
    self.assertEqual(j, d_("{'results': [{'values': [[2, 'declan', None]], 'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))

  def test_simple_parameterized_mixed_queries(self):
    '''Test a mix of parameterized and non-parameterized queries work as expected'''
    n = self.cluster.wait_for_leader()
    j = n.execute('CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)')
    self.assertEqual(j, d_("{'results': [{}]}"))

    body = json.dumps([
        ["INSERT INTO bar(name, age) VALUES(?,?)", "fiona", 20],
        ['INSERT INTO bar(name, age) VALUES("sinead", 25)']
    ])
    j = n.execute_raw(body)
    applied = n.wait_for_all_applied()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}, {'last_insert_id': 2, 'rows_affected': 1}]}"))
    j = n.query('SELECT * from bar')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona', 20], [2, 'sinead', 25]], 'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))

  def test_simple_parameterized_mixed_queries_via_request(self):
    '''Test a mix of parameterized and non-parameterized queries work as expected with unified endpoint'''
    n = self.cluster.wait_for_leader()
    j = n.execute('CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)')
    self.assertEqual(j, d_("{'results': [{}]}"))

    body = json.dumps([
        ["INSERT INTO bar(name, age) VALUES(?,?)", "fiona", 20],
        ['INSERT INTO bar(name, age) VALUES("sinead", 25)']
    ])
    j = n.request_raw(body)
    applied = n.wait_for_all_applied()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}, {'last_insert_id': 2, 'rows_affected': 1}]}"))
    j = n.request('SELECT * from bar')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona', 20], [2, 'sinead', 25]], 'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))
    j = n.request('SELECT * from bar', associative=True)
    self.assertEqual(j, d_("{'results': [{'types': {'age': 'integer', 'id': 'integer', 'name': 'text'}, 'rows': [{'age': 20, 'id': 1, 'name': 'fiona'}, {'age': 25, 'id': 2, 'name': 'sinead'}]}]}"))

  def test_snapshot(self):
    ''' Test that a node performs at least 1 snapshot automatically'''
    n = self.cluster.wait_for_leader()
    j = n.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = n.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))

    applied = n.wait_for_all_applied()

    # Wait for a snapshot to happen.
    timeout = 10
    t = 0
    while True:
      nSnaps = n.num_snapshots()
      if nSnaps > 0:
        return
      if t > timeout:
        raise Exception('timeout', nSnaps)
      time.sleep(1)
      t+=1

class TestSingleNode_SnapshotRequest(unittest.TestCase):
  def test_snapshot_request(self):
    ''' Test that a node performs a snapshot when requested'''
    n = Node(RQLITED_PATH, '0')
    n.start()
    n.wait_for_leader()
    j = n.execute('CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))

    n.snapshot()
    self.assertEqual(n.expvar()['http']['user_snapshots'], 1)

class TestSingleNodeLoadRestart(unittest.TestCase):
  ''' Test that a node can load a SQLite data set in binary format'''
  def test_load_binary(self):
    self.n = Node(RQLITED_PATH, '0',  raft_snap_threshold=8192, raft_snap_int="30s")
    self.n.start()
    n = self.n.wait_for_leader()
    self.n.restore('system_test/e2e/testdata/1000-numbers.db', fmt='binary')
    poll_query(self.n, 'SELECT COUNT(*) from test', d_("{'results': [{'values': [[1000]], 'types': ['integer'], 'columns': ['COUNT(*)']}]}"))

    # Ensure node can restart after loading and that the data is correct.
    self.n.stop()
    self.n.start()
    self.n.wait_for_leader()
    self.n.restore('system_test/e2e/testdata/1000-numbers.db', fmt='binary')
    poll_query(self.n, 'SELECT COUNT(*) from test', d_("{'results': [{'values': [[1000]], 'types': ['integer'], 'columns': ['COUNT(*)']}]}"))

  def tearDown(self):
    deprovision_node(self.n)

class TestSingleNodeBootRestart(unittest.TestCase):
  ''' Test that a node can boot using a SQLite data set'''
  def test(self):
    self.n = Node(RQLITED_PATH, '0',  raft_snap_threshold=8192, raft_snap_int="30s")
    self.n.start()
    n = self.n.wait_for_leader()
    j = self.n.boot('system_test/e2e/testdata/1000-numbers.db')
    j = self.n.query('SELECT COUNT(*) from test')
    self.assertEqual(j, d_("{'results': [{'values': [[1000]], 'types': ['integer'], 'columns': ['COUNT(*)']}]}"))

    # Ensure node can restart after booting and that the data is correct.
    self.n.stop()
    self.n.start()
    self.n.wait_for_leader()
    j = self.n.query('SELECT COUNT(*) from test')
    self.assertEqual(j, d_("{'results': [{'values': [[1000]], 'types': ['integer'], 'columns': ['COUNT(*)']}]}"))

  def tearDown(self):
    deprovision_node(self.n)

class TestSingleNodeReadyz(unittest.TestCase):
  def test(self):
    ''' Test /readyz behaves correctly'''
    n0 = Node(RQLITED_PATH, '0')
    n0.start(join="http://nonsense")
    self.assertEqual(False, n0.ready())
    self.assertEqual(True, n0.ready(noleader=True))
    self.assertEqual(False, n0.ready(noleader=False))
    self.assertEqual(False, n0.ready(sync=True))
    deprovision_node(n0)

class TestEndToEndSnapshotNoRestoreSingle(unittest.TestCase):
  def setUp(self):
    self.n0 = Node(RQLITED_PATH, '0',  raft_snap_threshold=10, raft_snap_int="1s")
    self.n0.start()
    self.n0.wait_for_leader()

  def waitForSnapIndex(self, n):
    timeout = 10
    t = 0
    while True:
      if t > timeout:
        raise Exception('timeout')
      if self.n0.raft_last_snapshot_index() >= n:
        break
      time.sleep(1)
      t+=1

  def test_snap_and_restart(self):
    '''Check that an node restarts correctly after multiple snapshots'''

    # Let's get multiple snapshots done.
    self.n0.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')

    for i in range(0,200):
      self.n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.n0.wait_for_all_applied()
    self.waitForSnapIndex(175)

    # Ensure node has the full correct state.
    j = self.n0.query('SELECT count(*) FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[200]], 'types': ['integer'], 'columns': ['count(*)']}]}"))

    # Restart node, and make sure it comes back with the correct state. It should skip restoring from
    # snapshot as it can use the SQLite database directly. This is the "startup optimization".
    self.n0.stop()
    self.n0.start()
    self.n0.wait_for_leader()
    self.n0.wait_for_all_applied()

    self.assertEqual(self.n0.num_available_snapshots(), 1)
    self.assertEqual(self.n0.expvar()['store']['num_restores_start_skipped'], 1)
    self.assertEqual(self.n0.expvar()['store']['num_restores_start'], 0)
    j = self.n0.query('SELECT count(*) FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[200]], 'types': ['integer'], 'columns': ['count(*)']}]}"))

  def tearDown(self):
    deprovision_node(self.n0)

if __name__ == "__main__":
  unittest.main(verbosity=2)
