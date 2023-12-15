#!/usr/bin/env python

import os
import unittest
from helpers import Node, Cluster, d_, deprovision_node, copy_dir_to_temp

RQLITED_PATH = os.environ['RQLITED_PATH']

class TestUpgrade_v7(unittest.TestCase):
  '''Test that a v7 cluster can be upgraded to this version code'''
  def test(self):
    dir1 = copy_dir_to_temp('testdata/v7/data.1')
    n0 = Node(RQLITED_PATH, '1', api_addr='localhost:4001', raft_addr='localhost:4002', dir=dir1)
    n0.start()

    dir2 = copy_dir_to_temp('testdata/v7/data.2')
    n1 = Node(RQLITED_PATH, '2', api_addr='localhost:4003', raft_addr='localhost:4004', dir=dir2)
    n1.start()

    dir3 = copy_dir_to_temp('testdata/v7/data.3')
    n2 = Node(RQLITED_PATH, '3', api_addr='localhost:4005', raft_addr='localhost:4006', dir=dir3)
    n2.start()

    self.cluster = Cluster([n0, n1, n2])
    l = self.cluster.wait_for_leader()

    # Check that each node upgraded a snapshot.
    for n in self.cluster.nodes:
      self.assertEqual(n.expvar()['snapshot']['upgrade_ok'], 1)

    # Check that each node has the right data.
    for n in self.cluster.nodes:
      self.assertEqual(n.query('SELECT COUNT(*) FROM foo', level='none'), d_("{'results': [{'values': [[28]], 'types': ['integer'], 'columns': ['COUNT(*)']}]}"))

  def tearDown(self):
    self.cluster.deprovision()

class TestUpgrade_v8_LoadChunk(unittest.TestCase):
  '''Test that a v8 node with chunked load commands in the log can be upgraded to this version code'''
  def test(self):
    dir = copy_dir_to_temp('testdata/v8/chunked')
    self.n = Node(RQLITED_PATH, 'localhost:4002', api_addr='localhost:4001', raft_addr='localhost:4002', dir=dir)
    self.n.start()
    l = self.n.wait_for_leader()
    self.assertEqual(self.n.query('SELECT COUNT(*) FROM foo', level='none'), d_("{'results': [{'values': [[1]], 'types': ['integer'], 'columns': ['COUNT(*)']}]}"))

  def tearDown(self):
    deprovision_node(self.n)

if __name__ == "__main__":
  unittest.main(verbosity=2)
