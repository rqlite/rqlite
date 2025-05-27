#!/usr/bin/env python
#
# End-to-end testing using actual rqlited binary.
#
# To run a specific test, execute
#
#  python system_test/full_system_test.py Class.test

import tempfile
import os
import unittest
import subprocess

from certs import x509cert, x509key
from helpers import Node, Cluster, d_, write_random_file, deprovision_node

RQLITED_PATH = os.environ['RQLITED_PATH']

class TestJoinEncryptedNoVerify(unittest.TestCase):
  def test(self):
    ''' Test that a joining node will not operate if remote cert can't be trusted'''
    certFile = write_random_file(x509cert)
    keyFile = write_random_file(x509key)

    n0 = Node(RQLITED_PATH, '0', node_cert=certFile, node_key=keyFile, node_no_verify=False)
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1', node_cert=certFile, node_key=keyFile, node_no_verify=False)
    n1.start(join=n0.RaftAddr())
    self.assertRaises(Exception, n1.wait_for_leader) # Join should fail due to bad cert.

    deprovision_node(n0)
    deprovision_node(n1)

class TestAuthJoin(unittest.TestCase):
  '''Test that joining works with authentication'''

  def test(self):
    self.auth_file = tempfile.NamedTemporaryFile()
    with open(self.auth_file.name, 'w') as f:
      f.write('''
[
    {
        "username": "foo",
        "password": "bar",
        "perms": [
            "all"
        ]
    },
    {
        "username": "qux",
        "password": "baz",
        "perms": [
            "join-read-only"
        ]
    },
    {
        "username": "*",
        "perms": [
            "status",
            "ready"
        ]
    }
]''')

    n0 = Node(RQLITED_PATH, '0', auth=self.auth_file.name)
    n0.start()
    n0.wait_for_leader()
    self.assertEqual(len(n0.nodes()), 1)

    n1 = Node(RQLITED_PATH, '1', auth=self.auth_file.name)
    n1.start(join=n0.RaftAddr())
    self.assertTrue(n1.expect_leader_fail()) # Join should fail due to lack of auth.
    self.assertEqual(len(n0.nodes()), 1)

    n2 = Node(RQLITED_PATH, '2', auth=self.auth_file.name)
    n2.start(join=n0.RaftAddr(), join_as="foo")
    n2.wait_for_leader()
    self.assertEqual(len(n0.nodes()), 2)
    self.assertTrue(n2.is_voter())

    n3 = Node(RQLITED_PATH, '3', auth=self.auth_file.name)
    n3.start(join=n0.RaftAddr(), join_as="qux")
    self.assertTrue(n3.expect_leader_fail()) # Join should fail due to lack of auth.
    self.assertEqual(len(n0.nodes()), 2)

    n4 = Node(RQLITED_PATH, '4', auth=self.auth_file.name, raft_voter=False)
    n4.start(join=n0.RaftAddr(), join_as="qux")
    n4.wait_for_leader()
    self.assertEqual(len(n0.nodes()), 3)

    self.cluster = Cluster([n0, n1, n2, n3, n4])

  def tearDown(self):
    self.auth_file.close()
    if hasattr(self, 'cluster'):
      self.cluster.deprovision()

class TestIdempotentJoin(unittest.TestCase):
  def tearDown(self):
    deprovision_node(self.n0)
    deprovision_node(self.n1)

  def test(self):
    '''Test that a node performing two join requests works fine'''
    self.n0 = Node(RQLITED_PATH, '0')
    self.n0.start()
    self.n0.wait_for_leader()

    self.n1 = Node(RQLITED_PATH, '1')
    self.n1.start(join=self.n0.RaftAddr())
    self.n1.wait_for_leader()

    self.assertEqual(self.n0.num_join_requests(), 1)

    self.n1.stop()
    self.n1.start(join=self.n0.RaftAddr())
    self.n1.wait_for_leader()
    self.assertEqual(self.n0.num_join_requests(), 2)


class TestForwardedJoin(unittest.TestCase):
  def tearDown(self):
    deprovision_node(self.n0)
    deprovision_node(self.n1)
    deprovision_node(self.n2)

  def test(self):
    '''Test that a node can join via a follower'''
    self.n0 = Node(RQLITED_PATH, '0')
    self.n0.start()
    l0 = self.n0.wait_for_leader()

    self.n1 = Node(RQLITED_PATH, '1')
    self.n1.start(join=self.n0.RaftAddr())
    self.n1.wait_for_leader()
    self.assertTrue(self.n1.is_follower())

    self.n2 = Node(RQLITED_PATH, '2')
    self.n2.start(join=self.n1.RaftAddr())
    l2 = self.n2.wait_for_leader()
    self.assertEqual(l0, l2)

  def test_raft_adv(self):
    '''Test that a node can join via a follower that advertises a different Raft address'''
    self.n0 = Node(RQLITED_PATH, '0',
      raft_addr="0.0.0.0:4002", raft_adv="localhost:4002")
    self.n0.start()
    l0 = self.n0.wait_for_leader()

    self.n1 = Node(RQLITED_PATH, '1')
    self.n1.start(join=self.n0.RaftAddr())
    self.n1.wait_for_leader()
    self.assertTrue(self.n1.is_follower())

    self.n2 = Node(RQLITED_PATH, '2')
    self.n2.start(join=self.n1.RaftAddr())
    l2 = self.n2.wait_for_leader()
    self.assertEqual(l0, l2)

class TestSingleNodeJoin(unittest.TestCase):
  def setUp(self):
    # Setup single node
    self.n0 = Node(RQLITED_PATH, '0')
    self.n0.start()
    self.n0.wait_for_leader()
    
    # Create a potential join target node
    self.n1 = Node(RQLITED_PATH, '1')
    self.n1.start()
    self.n1.wait_for_leader()

  def test_single_node_join_fails(self):
    '''Test that a single-node cluster leader cannot join another cluster'''
    # Check that n0 is a single-node cluster
    nodes = self.n0.nodes()
    self.assertEqual(len(nodes), 1)
    
    # Stop the node gracefully
    self.n0.stop(graceful=True)
    
    # Attempt to restart with join flag - this should exit with an error
    try:
      self.n0.start(join=self.n1.RaftAddr())
      self.fail("Expected an exception when trying to join single-node cluster to another cluster")
    except Exception:
      # Expected behavior - the node should fail to start
      pass
      
    # Verify that the process is not running
    self.assertFalse(self.n0.running())

  def tearDown(self):
    deprovision_node(self.n0)
    deprovision_node(self.n1)

class TestJoinCatchup(unittest.TestCase):
  def setUp(self):
    self.n0 = Node(RQLITED_PATH, '0')
    self.n0.start()
    self.n0.wait_for_leader()

    self.n1 = Node(RQLITED_PATH, '1')
    self.n1.start(join=self.n0.RaftAddr())
    self.n1.wait_for_leader()

    self.n2 = Node(RQLITED_PATH, '2')
    self.n2.start(join=self.n0.RaftAddr())
    self.n2.wait_for_leader()

    self.cluster = Cluster([self.n0, self.n1, self.n2])

  def tearDown(self):
    self.cluster.deprovision()

  def test_no_change_id_addr(self):
    '''Test that a node rejoining without changing ID or address picks up changes'''
    n0 = self.cluster.wait_for_leader()
    j = n0.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = n0.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))
    applied = n0.wait_for_all_applied()

    # Test that follower node has correct state in local database, and then kill the follower
    self.n1.wait_for_fsm_index(applied)
    j = self.n1.query('SELECT * FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))
    self.n1.stop()

    # Insert a new record
    j = n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 2, 'rows_affected': 1}]}"))
    j = n0.query('SELECT COUNT(*) FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[2]], 'types': ['integer'], 'columns': ['COUNT(*)']}]}"))
    applied = n0.wait_for_all_applied()

    # Restart follower, explicitly rejoin, and ensure it picks up new records
    self.n1.start(join=self.n0.RaftAddr())
    self.n1.wait_for_leader()
    self.n1.wait_for_fsm_index(applied)
    self.assertEqual(n0.expvar()['store']['num_ignored_joins'], 1)
    j = self.n1.query('SELECT COUNT(*) FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[2]], 'types': ['integer'], 'columns': ['COUNT(*)']}]}"))

  def test_change_addresses(self):
    '''Test that a node rejoining with new addresses works fine'''
    n0 = self.cluster.wait_for_leader()
    j = n0.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = n0.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))
    applied = n0.wait_for_all_applied()

    # Test that follower node has correct state in local database, and then kill the follower
    self.n1.wait_for_fsm_index(applied)
    j = self.n1.query('SELECT * FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))
    self.n1.stop()

    # Insert a new record
    j = n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 2, 'rows_affected': 1}]}"))
    j = n0.query('SELECT COUNT(*) FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[2]], 'types': ['integer'], 'columns': ['COUNT(*)']}]}"))
    applied = n0.wait_for_all_applied()

    # Restart follower with new network attributes, explicitly rejoin, and ensure it picks up new records
    self.n1.scramble_network()
    self.n1.start(join=self.n0.RaftAddr())
    self.n1.wait_for_leader()
    self.assertEqual(n0.expvar()['store']['num_removed_before_joins'], 1)
    self.n1.wait_for_fsm_index(applied)
    j = self.n1.query('SELECT COUNT(*) FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[2]], 'types': ['integer'], 'columns': ['COUNT(*)']}]}"))

if __name__ == "__main__":
  unittest.main(verbosity=2)