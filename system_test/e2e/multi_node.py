#!/usr/bin/env python
#
# End-to-end testing using actual rqlited binary.
#
# To run a specific test, execute
#
#  python system_test/full_system_test.py Class.test

import tempfile
import os
import time
import sqlite3
import unittest

from certs import x509cert, x509key
from helpers import Node, Cluster, d_, write_random_file, deprovision_node, is_sequence_number, TIMEOUT

RQLITED_PATH = os.environ['RQLITED_PATH']

class TestEndToEnd(unittest.TestCase):
  def setUp(self):
    n0 = Node(RQLITED_PATH, '0')
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1')
    n1.start(join=n0.RaftAddr())
    n1.wait_for_leader()

    n2 = Node(RQLITED_PATH, '2')
    n2.start(join=n0.RaftAddr())
    n2.wait_for_leader()

    self.cluster = Cluster([n0, n1, n2])

  def tearDown(self):
    self.cluster.deprovision()

  def test_election(self):
    '''Test basic leader election'''

    n = self.cluster.wait_for_leader().stop()

    self.cluster.wait_for_leader(node_exc=n)
    n.start()
    n.wait_for_leader()

  def test_full_restart(self):
    '''Test that a cluster can perform a full restart successfully'''
    self.cluster.wait_for_leader()
    pids = set(self.cluster.pids())

    self.cluster.stop()
    self.cluster.start()
    self.cluster.wait_for_leader()
    self.cluster.cross_check_leader()
    # Guard against any error in testing, by confirming that restarting the cluster
    # actually resulted in all new rqlited processes.
    self.assertTrue(pids.isdisjoint(set(self.cluster.pids())))

  def test_execute_fail_restart(self):
    '''Test that a node that fails picks up changes after restarting'''

    n = self.cluster.wait_for_leader()
    j = n.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = n.execute('INSERT INTO foo(name) VALUES("fiona")')
    fsmIdx = n.wait_for_all_fsm()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = n.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    n0 = self.cluster.wait_for_leader().stop()
    n1 = self.cluster.wait_for_leader(node_exc=n0)
    n1.wait_for_fsm_index(fsmIdx)
    j = n1.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))
    j = n1.execute('INSERT INTO foo(name) VALUES("declan")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 2, 'rows_affected': 1}]}"))

    n0.start()
    n0.wait_for_leader()
    n0.wait_for_fsm_index(n1.fsm_index())
    j = n0.query('SELECT * FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona'], [2, 'declan']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

  def test_leader_redirect(self):
    '''Test that followers supply the correct leader redirects (HTTP 301)'''

    l = self.cluster.wait_for_leader()
    fs = self.cluster.followers()
    self.assertEqual(len(fs), 2)
    for n in fs:
      self.assertEqual(l.APIProtoAddr(), n.redirect_addr())

    # Kill the leader, wait for a new leader, and check that the
    # redirect returns the new leader address.
    l.stop()
    n = self.cluster.wait_for_leader(node_exc=l)
    for f in self.cluster.followers():
      self.assertEqual(n.APIProtoAddr(), f.redirect_addr())

  def test_nodes(self):
    '''Test that the nodes/ endpoint operates as expected'''
    l = self.cluster.wait_for_leader()
    fs = self.cluster.followers()
    self.assertEqual(len(fs), 2)

    nodes = l.nodes()
    self.assertEqual(nodes[l.node_id]['leader'], True)
    self.assertEqual(nodes[l.node_id]['reachable'], True)
    self.assertEqual(nodes[l.node_id]['api_addr'], l.APIProtoAddr())
    self.assertTrue('time' in nodes[fs[0].node_id])
    for n in [fs[0], fs[1]]:
      self.assertEqual(nodes[n.node_id]['leader'], False)
      self.assertEqual(nodes[n.node_id]['reachable'], True)
      self.assertEqual(nodes[n.node_id]['api_addr'], n.APIProtoAddr())
      self.assertTrue('time' in nodes[n.node_id])
      self.assertTrue('time' in nodes[fs[0].node_id])

    fs[0].stop()
    nodes = l.nodes()
    self.assertEqual(nodes[fs[0].node_id]['reachable'], False)
    self.assertTrue('error' in nodes[fs[0].node_id])
    self.assertEqual(nodes[fs[1].node_id]['reachable'], True)
    self.assertEqual(nodes[l.node_id]['reachable'], True)

  def test_remove_node_via_leader(self):
    '''Test that removing a node via the leader works'''
    l = self.cluster.wait_for_leader()
    fs = self.cluster.followers()

    # Validate state of cluster.
    self.assertEqual(len(fs), 2)
    nodes = l.nodes()
    self.assertEqual(len(nodes), 3)

    l.remove_node(fs[0].node_id)
    nodes = l.nodes()
    self.assertEqual(len(nodes), 2)

  def test_remove_node_via_follower(self):
    '''Test that removing a node via a follower works'''
    l = self.cluster.wait_for_leader()
    fs = self.cluster.followers()

    # Validate state of cluster.
    self.assertEqual(len(fs), 2)
    nodes = l.nodes()
    self.assertEqual(len(nodes), 3)

    fs[0].remove_node(fs[1].node_id)
    nodes = l.nodes()
    self.assertEqual(len(nodes), 2)

class TestEndToEndEncryptedNode(TestEndToEnd):
  def setUp(self):
    certFile = write_random_file(x509cert)
    keyFile = write_random_file(x509key)

    n0 = Node(RQLITED_PATH, '0', node_cert=certFile, node_key=keyFile, node_no_verify=True)
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1', node_cert=certFile, node_key=keyFile, node_no_verify=True)
    n1.start(join=n0.RaftAddr())
    n1.wait_for_leader()

    n2 = Node(RQLITED_PATH, '2', node_cert=certFile, node_key=keyFile, node_no_verify=True)
    n2.start(join=n0.RaftAddr())
    n2.wait_for_leader()

    self.cluster = Cluster([n0, n1, n2])

class TestClusterRecovery(unittest.TestCase):
  '''Test that a cluster can recover after all Raft network addresses change'''
  def test(self):
    n0 = Node(RQLITED_PATH, '0')
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1')
    n1.start(join=n0.RaftAddr())
    n1.wait_for_leader()

    n2 = Node(RQLITED_PATH, '2')
    n2.start(join=n0.RaftAddr())
    n2.wait_for_leader()

    self.nodes = [n0, n1, n2]

    j = n0.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    fsmIdx = n0.wait_for_all_fsm()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = n0.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    n0.stop()
    n1.stop()
    n2.stop()

    # Create new cluster from existing data, but with new network addresses.
    addr3 = "127.0.0.1:10003"
    addr4 = "127.0.0.1:10004"
    addr5 = "127.0.0.1:10005"

    peers = [
      {"id": n0.node_id, "address": addr3},
      {"id": n1.node_id, "address": addr4},
      {"id": n2.node_id, "address": addr5},
    ]

    n3 = Node(RQLITED_PATH, n0.node_id, dir=n0.dir, raft_addr=addr3)
    n4 = Node(RQLITED_PATH, n1.node_id, dir=n1.dir, raft_addr=addr4)
    n5 = Node(RQLITED_PATH, n2.node_id, dir=n2.dir, raft_addr=addr5)
    self.nodes = self.nodes + [n3, n4, n5]

    n3.set_peers(peers)
    n4.set_peers(peers)
    n5.set_peers(peers)

    n3.start(wait=False)
    n4.start(wait=False)
    n5.start(wait=False)

    # New cluster ok?
    n3.wait_for_leader()

    j = n3.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

  def tearDown(self):
    for n in self.nodes:
      deprovision_node(n)

class TestRequestForwarding(unittest.TestCase):
  '''Test that followers transparently forward requests to leaders'''

  def setUp(self):
    n0 = Node(RQLITED_PATH, '0')
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1')
    n1.start(join=n0.RaftAddr())
    n1.wait_for_leader()

    self.cluster = Cluster([n0, n1])

  def tearDown(self):
    self.cluster.deprovision()

  def test_execute_query_forward(self):
      l = self.cluster.wait_for_leader()
      j = l.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
      self.assertEqual(j, d_("{'results': [{}]}"))

      f = self.cluster.followers()[0]
      j = f.execute('INSERT INTO foo(name) VALUES("fiona")')
      self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
      fsmIdx = l.wait_for_all_fsm()

      j = l.query('SELECT * FROM foo')
      self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))
      j = f.query('SELECT * FROM foo')
      self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))
      self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))
      j = f.query('SELECT * FROM foo', level="strong")
      self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

  def test_execute_queued_forward(self):
      l = self.cluster.wait_for_leader()
      j = l.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
      self.assertEqual(j, d_("{'results': [{}]}"))

      f = self.cluster.followers()[0]
      j = f.execute('INSERT INTO foo(name) VALUES("fiona")')
      self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
      fsmIdx = l.wait_for_all_fsm()

      j = f.execute_queued('INSERT INTO foo(name) VALUES("declan")')
      self.assertTrue(is_sequence_number(str(j)))

      j = f.execute_queued('INSERT INTO foo(name) VALUES(?)', params=["aoife"])
      self.assertTrue(is_sequence_number(str(j)))

      # Wait for queued write to happen.
      timeout = 10
      t = 0
      while True:
        j = l.query('SELECT * FROM foo')
        if j == d_("{'results': [{'values': [[1, 'fiona'], [2, 'declan'], [3, 'aoife']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"):
          break
        if t > timeout:
          raise Exception('timeout')
        time.sleep(1)
        t+=1

  def test_execute_queued_forward_wait(self):
      l = self.cluster.wait_for_leader()
      j = l.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
      self.assertEqual(j, d_("{'results': [{}]}"))

      f = self.cluster.followers()[0]
      j = f.execute('INSERT INTO foo(name) VALUES("fiona")')
      self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
      fsmIdx = l.wait_for_all_fsm()

      # Load up the queue!
      for i in range(0,2000):
        j = f.execute_queued('INSERT INTO foo(name) VALUES("declan")')
        self.assertTrue(is_sequence_number(str(j)))

      j = f.execute_queued('INSERT INTO foo(name) VALUES(?)', wait=True, params=["aoife"])
      self.assertTrue(is_sequence_number(str(j)))

      # Data should be ready immediately, since we waited.
      j = l.query('SELECT COUNT(*) FROM foo')
      self.assertEqual(j, d_("{'results': [{'columns': ['COUNT(*)'], 'types': ['integer'], 'values': [[2002]]}]}"))

class TestEndToEndNonVoter(unittest.TestCase):
  def setUp(self):
    self.leader = Node(RQLITED_PATH, '0')
    self.leader.start()
    self.leader.wait_for_leader()

    self.non_voter = Node(RQLITED_PATH, '1', raft_voter=False)
    self.non_voter.start(join=self.leader.RaftAddr())
    self.non_voter.wait_for_leader()

    self.cluster = Cluster([self.leader, self.non_voter])

  def tearDown(self):
    self.cluster.deprovision()

  def test_execute_fail_rejoin(self):
    '''Test that a non-voter that fails can rejoin the cluster, and pick up changes'''

    # Confirm that voting status reporting is correct
    self.assertTrue(self.leader.is_voter())
    self.assertFalse(self.non_voter.is_voter())

    # Insert some records via the leader
    j = self.leader.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = self.leader.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.leader.wait_for_all_fsm()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = self.leader.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    # Stop non-voter and then insert some more records
    self.non_voter.stop()
    j = self.leader.execute('INSERT INTO foo(name) VALUES("declan")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 2, 'rows_affected': 1}]}"))

    # Restart non-voter and confirm it picks up changes
    self.non_voter.start()
    self.non_voter.wait_for_leader()
    self.non_voter.wait_for_fsm_index(self.leader.fsm_index())
    j = self.non_voter.query('SELECT * FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona'], [2, 'declan']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

  def test_leader_redirect(self):
    '''Test that non-voters supply the correct leader redirects (HTTP 301)'''

    l = self.cluster.wait_for_leader()
    fs = self.cluster.followers()
    self.assertEqual(len(fs), 1)
    for n in fs:
      self.assertEqual(l.APIProtoAddr(), n.redirect_addr())

    l.stop()
    n = self.cluster.wait_for_leader(node_exc=l)
    for f in self.cluster.followers():
      self.assertEqual(n.APIProtoAddr(), f.redirect_addr())

class TestEndToEndNonVoterFollowsLeader(unittest.TestCase):
  def setUp(self):
    n0 = Node(RQLITED_PATH, '0')
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1')
    n1.start(join=n0.RaftAddr())
    n1.wait_for_leader()

    n2 = Node(RQLITED_PATH, '2')
    n2.start(join=n0.RaftAddr())
    n2.wait_for_leader()

    self.non_voter = Node(RQLITED_PATH, '3', raft_voter=False)
    self.non_voter.start(join=n0.RaftAddr())
    self.non_voter.wait_for_leader()

    self.cluster = Cluster([n0, n1, n2, self.non_voter])

  def tearDown(self):
    self.cluster.deprovision()

  def test_execute_fail_nonvoter(self):
    '''Test that a non-voter always picks up changes, even when leader changes'''

    n = self.cluster.wait_for_leader()
    j = n.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = n.execute('INSERT INTO foo(name) VALUES("fiona")')
    n.wait_for_all_fsm()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = n.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    # Kill leader, and then make more changes.
    n0 = self.cluster.wait_for_leader().stop()
    n1 = self.cluster.wait_for_leader(node_exc=n0)
    n1.wait_for_all_applied()
    n1.query('SELECT count(*) FROM foo', level='strong') ## Send one more operation through log before query.
    j = n1.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))
    j = n1.execute('INSERT INTO foo(name) VALUES("declan")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 2, 'rows_affected': 1}]}"))

    # Confirm non-voter sees changes made through old and new leader.
    self.non_voter.wait_for_fsm_index(n1.fsm_index())
    j = self.non_voter.query('SELECT * FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona'], [2, 'declan']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

class TestEndToEndBackupRestore(unittest.TestCase):
  def test_backup_restore(self):
    fd, self.db_file = tempfile.mkstemp()
    os.close(fd)

    # Create a two-node cluster.
    self.node0 = Node(RQLITED_PATH, '0')
    self.node0.start()
    self.node0.wait_for_leader()
    self.node0.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.node0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.node0.wait_for_all_fsm()
    self.node1 = Node(RQLITED_PATH, '1')
    self.node1.start(join=self.node0.RaftAddr())
    self.node1.wait_for_leader()

    # Get a backup from the first node and check it.
    self.node0.backup(self.db_file)
    conn = sqlite3.connect(self.db_file)
    rows = conn.execute('SELECT * FROM foo').fetchall()
    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0], (1, 'fiona'))
    conn.close()

    # Get a backup from the other node and check it too.
    self.node1.backup(self.db_file)
    conn = sqlite3.connect(self.db_file)
    rows = conn.execute('SELECT * FROM foo').fetchall()
    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0], (1, 'fiona'))
    conn.close()

    # Load file into a brand new single node, check the data is right.
    self.node2 = Node(RQLITED_PATH, '3')
    self.node2.start()
    self.node2.wait_for_leader()
    j = self.node2.restore(self.db_file)
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = self.node2.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    # Start another 2-node cluster, load data via follower and check.
    self.node3 = Node(RQLITED_PATH, '3')
    self.node3.start()
    self.node3.wait_for_leader()
    self.node4 = Node(RQLITED_PATH, '4')
    self.node4.start(join=self.node3.RaftAddr())
    self.node4.wait_for_leader()
    self.assertTrue(self.node3.is_leader())

    self.node4.restore(self.db_file, fmt='binary')
    self.node3.wait_for_all_fsm()
    j = self.node3.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

  def tearDown(self):
    if hasattr(self, 'node0'):
      deprovision_node(self.node0)
    if hasattr(self, 'node1'):
      deprovision_node(self.node1)
    if hasattr(self, 'node2'):
      deprovision_node(self.node2)
    if hasattr(self, 'node3'):
      deprovision_node(self.node3)
    if hasattr(self, 'node4'):
      deprovision_node(self.node4)
    os.remove(self.db_file)

class TestEndToEndSnapRestoreCluster(unittest.TestCase):
  def wait_for_snap(self, n):
    timeout = 10
    t = 0
    while True:
      if t > timeout:
        raise Exception('timeout')
      if self.n0.num_snapshots() is n:
        break
      time.sleep(1)
      t+=1

  def poll_query(self, node, exp):
    t = 0
    while True:
      if t > TIMEOUT:
        raise Exception('timeout waiting for node %s to have all data' % node.node_id)
      j = node.query('SELECT count(*) FROM foo', level='none')
      if j == d_("{'results': [{'values': [[502]], 'types': ['integer'], 'columns': ['count(*)']}]}"):
        break
      time.sleep(1)
      t+=1

  def test_join_with_snap(self):
    '''Check that nodes join a cluster correctly via a snapshot'''
    self.n0 = Node(RQLITED_PATH, '0',  raft_snap_threshold=100, raft_snap_int="1s")
    self.n0.start()
    self.n0.wait_for_leader()
    self.n0.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')

    # Let's get multiple snapshots done.
    for j in range(1, 4):
      for i in range(0,100):
        self.n0.execute('INSERT INTO foo(name) VALUES("fiona")')
      self.n0.wait_for_all_fsm()
      self.wait_for_snap(j)

    # Add two more nodes to the cluster
    self.n1 = Node(RQLITED_PATH, '1')
    self.n1.start(join=self.n0.RaftAddr())
    self.n1.wait_for_leader()

    self.n2 = Node(RQLITED_PATH, '2')
    self.n2.start(join=self.n0.RaftAddr())
    self.n2.wait_for_leader()

    # Force the Apply loop to run on the node, so fsm_index is updated.
    self.n0.execute('INSERT INTO foo(name) VALUES("fiona")')

    # Ensure those new nodes have the full correct state.
    self.n1.wait_for_fsm_index(self.n0.fsm_index())
    j = self.n1.query('SELECT count(*) FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[301]], 'types': ['integer'], 'columns': ['count(*)']}]}"))

    self.n2.wait_for_fsm_index(self.n0.fsm_index())
    j = self.n2.query('SELECT count(*) FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[301]], 'types': ['integer'], 'columns': ['count(*)']}]}"))

    # Kill one of the nodes, and make more changes, enough to trigger more snaps.
    self.n2.stop()

    # Let's get more snapshots done.
    for j in range(3, 5):
      for i in range(0,100):
        self.n0.execute('INSERT INTO foo(name) VALUES("fiona")')
      self.n0.wait_for_all_fsm()
      self.wait_for_snap(j)

    # Restart killed node, check it has full state.
    self.n2.start()
    self.n2.wait_for_leader()

    # Force the Apply loop to run on the node twice, so fsm_index is updated on n2 to a point
    # where the SQLite database on n2 is updated.
    self.n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.n2.query('SELECT count(*) FROM foo', level='strong')

    # Snapshot testing is tricky, as it's so timing-based -- and therefore hard to predict.
    # So adopt a polling approach.
    self.poll_query(self.n2, d_("{'results': [{'values': [[502]], 'types': ['integer'], 'columns': ['count(*)']}]}"))

    # Launch a brand-new node, and check that is catches up via a single restore from snapshot.
    self.n3 = Node(RQLITED_PATH, '3')
    self.n3.start(join=self.n0.RaftAddr())
    self.n3.wait_for_leader()
    self.poll_query(self.n3, d_("{'results': [{'values': [[502]], 'types': ['integer'], 'columns': ['count(*)']}]}"))
    self.assertEqual(self.n3.expvar()['store']['num_restores'], 1)

  def tearDown(self):
    deprovision_node(self.n0)
    deprovision_node(self.n1)
    deprovision_node(self.n2)
    deprovision_node(self.n3)

class TestShutdown(unittest.TestCase):
  def test_cluster_leader_remove_on_shutdown(self):
    '''Test that removing the leader on shutdown leaves a good cluster'''
    n0 = Node(RQLITED_PATH, '0',  raft_cluster_remove_shutdown=True)
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1', raft_cluster_remove_shutdown=True)
    n1.start(join=n0.RaftAddr())
    n1.wait_for_leader()

    nodes = n0.nodes()
    self.assertEqual(len(nodes), 2)
    nodes = n1.nodes()
    self.assertEqual(len(nodes), 2)

    n0.stop(graceful=True)
    nodes = n1.nodes()
    self.assertEqual(len(nodes), 1)

    # Check that we have a working single-node cluster with a leader by doing
    # a write.
    n1.wait_for_ready()
    j = n1.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))

    deprovision_node(n0)
    deprovision_node(n1)

  def test_cluster_follower_remove_on_shutdown(self):
    '''Test that removing a follower on shutdown leaves a good cluster'''
    n0 = Node(RQLITED_PATH, '0',  raft_cluster_remove_shutdown=True)
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1', raft_cluster_remove_shutdown=True)
    n1.start(join=n0.RaftAddr())
    n1.wait_for_leader()

    nodes = n0.nodes()
    self.assertEqual(len(nodes), 2)
    nodes = n1.nodes()
    self.assertEqual(len(nodes), 2)

    n1.stop(graceful=True)
    nodes = n0.nodes()
    self.assertEqual(len(nodes), 1)

    # Check that we have a working single-node cluster with a leader by doing
    # a write.
    n0.wait_for_ready()
    j = n0.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))

    deprovision_node(n0)
    deprovision_node(n1)

if __name__ == "__main__":
  unittest.main(verbosity=2)

