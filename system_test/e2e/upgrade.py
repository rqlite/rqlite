#!/usr/bin/env python

import os, unittest, time
from helpers import Node, Cluster, d_, deprovision_node, copy_dir_to_temp, poll_query

RQLITED_PATH = os.environ['RQLITED_PATH']
TIMEOUT=10

class TestUpgrade_v7(unittest.TestCase):
  '''Test that a v7 cluster can be upgraded to this version code'''
  def poll_query(self, node, query, exp):
    t = 0
    j = None
    while True:
      if t > TIMEOUT:
        raise Exception('timeout waiting for node %s to return correct results (got %s)' % (node.node_id, j))
      j = node.query(query, level='none')
      if j == exp:
        break
      time.sleep(1)
      t+=1

  def test(self):
    dir1 = copy_dir_to_temp('testdata/v7/data.1')
    n0 = Node(RQLITED_PATH, '1', api_addr='localhost:4001', raft_addr='localhost:4002', dir=dir1,
              raft_snap_int='100ms', raft_snap_threshold='5')
    n0.start()

    dir2 = copy_dir_to_temp('testdata/v7/data.2')
    n1 = Node(RQLITED_PATH, '2', api_addr='localhost:4003', raft_addr='localhost:4004', dir=dir2,
              raft_snap_int='100ms', raft_snap_threshold='5')
    n1.start()

    dir3 = copy_dir_to_temp('testdata/v7/data.3')
    n2 = Node(RQLITED_PATH, '3', api_addr='localhost:4005', raft_addr='localhost:4006', dir=dir3,
              raft_snap_int='100ms', raft_snap_threshold='5')
    n2.start()

    self.cluster = Cluster([n0, n1, n2])
    l = self.cluster.wait_for_leader()

    # Check that each node performed the upgrade. The upgrade will first upgrade to v8 format
    # and then upgrade to v10 format (v9 is the same as v8 in terms of data format). Hence
    # we expect to see 2 upgrades counted in the expvar for each node.
    for n in self.cluster.nodes:
      self.assertEqual(n.expvar()['snapshot']['upgrade_ok'], 2)

    # Check that each node has the right data.
    for n in self.cluster.nodes:
      self.poll_query(n, 'SELECT COUNT(*) FROM foo', d_("{'results': [{'values': [[28]], 'types': ['integer'], 'columns': ['COUNT(*)']}]}"))

    # Check that writes work OK post upgrade and subsequent snapshots.
    for i in range(100):
      l = self.cluster.wait_for_leader()
      l.execute('INSERT INTO foo(name) VALUES("fiona")')

    # Check that each node has the right data.
    for n in self.cluster.nodes:
      self.poll_query(n, 'SELECT COUNT(*) FROM foo', d_("{'results': [{'values': [[128]], 'types': ['integer'], 'columns': ['COUNT(*)']}]}"))

  def tearDown(self):
    self.cluster.deprovision()

class TestUpgrade_v8_LoadChunk(unittest.TestCase):
  '''Test that a v8 node with chunked load commands in the log can be upgraded to this version code'''
  def test(self):
    dir = copy_dir_to_temp('testdata/v8/chunked')
    self.n = Node(RQLITED_PATH, 'localhost:4002', api_addr='localhost:4001', raft_addr='localhost:4002', dir=dir)
    self.n.start()
    l = self.n.wait_for_leader()
    poll_query(self.n, 'SELECT COUNT(*) FROM foo', d_("{'results': [{'values': [[1]], 'types': ['integer'], 'columns': ['COUNT(*)']}]}"))

  def tearDown(self):
    deprovision_node(self.n)

class TestUpgrade_v9(unittest.TestCase):
  '''Test that a v8 cluster can be upgraded to this version code'''
  def poll_query(self, node, query, exp):
    t = 0
    j = None
    while True:
      if t > TIMEOUT:
        raise Exception('timeout waiting for node %s to return correct results (got %s)' % (node.node_id, j))
      j = node.query(query, level='none')
      if j == exp:
        break
      time.sleep(1)
      t+=1

  def test(self):
    dir1 = copy_dir_to_temp('testdata/v9/data.1')
    n0 = Node(RQLITED_PATH, '1', api_addr='localhost:4001', raft_addr='localhost:4002', dir=dir1,
              raft_snap_int='100ms', raft_snap_threshold='5')
    n0.start()

    dir2 = copy_dir_to_temp('testdata/v9/data.2')
    n1 = Node(RQLITED_PATH, '2', api_addr='localhost:4003', raft_addr='localhost:4004', dir=dir2,
              raft_snap_int='100ms', raft_snap_threshold='5')
    n1.start()

    dir3 = copy_dir_to_temp('testdata/v9/data.3')
    n2 = Node(RQLITED_PATH, '3', api_addr='localhost:4005', raft_addr='localhost:4006', dir=dir3,
              raft_snap_int='100ms', raft_snap_threshold='5')
    n2.start()

    self.cluster = Cluster([n0, n1, n2])
    l = self.cluster.wait_for_leader()

    # Only node 1 has a snapshot to upgrade.
    self.assertEqual(n0.expvar()['snapshot']['upgrade_ok'], 1)
    self.assertEqual(n1.expvar()['snapshot']['upgrade_ok'], 0)
    self.assertEqual(n2.expvar()['snapshot']['upgrade_ok'], 0)

    # Check that each node has the right data.
    for n in self.cluster.nodes:
      self.poll_query(n, 'SELECT COUNT(*) FROM foo', d_("{'results': [{'values': [[10]], 'types': ['integer'], 'columns': ['COUNT(*)']}]}"))

    # Check that writes work OK post upgrade and subsequent snapshots.
    for i in range(100):
      l = self.cluster.wait_for_leader()
      l.execute('INSERT INTO foo(name) VALUES("fiona")')

    # Check that each node has the right data.
    for n in self.cluster.nodes:
      self.poll_query(n, 'SELECT COUNT(*) FROM foo', d_("{'results': [{'values': [[110]], 'types': ['integer'], 'columns': ['COUNT(*)']}]}"))

  def tearDown(self):
    self.cluster.deprovision()

if __name__ == "__main__":
  unittest.main(verbosity=2)
