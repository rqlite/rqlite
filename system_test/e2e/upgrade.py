#!/usr/bin/env python

import os
import unittest
from helpers import Node, Cluster, d_

RQLITED_PATH = os.environ['RQLITED_PATH']

class TestUpgrade_v7(unittest.TestCase):
  '''Test that a v7 cluster can be upgraded to this version code'''
  def test(self):
    n0 = Node(RQLITED_PATH, '1', api_addr='localhost:4001', raft_addr='localhost:4002', dir='testdata/v7/data.1')
    n0.start()

    n1 = Node(RQLITED_PATH, '2', api_addr='localhost:4003', raft_addr='localhost:4004', dir='testdata/v7/data.2')
    n1.start()

    n2 = Node(RQLITED_PATH, '3', api_addr='localhost:4005', raft_addr='localhost:4006', dir='testdata/v7/data.3')
    n2.start()

    self.cluster = Cluster([n0, n1, n2])
    l = self.cluster.wait_for_leader()

    # Check that each node upgraded a snapshot.
    for n in self.cluster.nodes:
      self.assertTrue(n.expvar()['snapshot']['upgrade_ok'] == 1)

    # Check that each node has the right data.
    for n in self.cluster.nodes:
      self.assertEqual(n.query('SELECT COUNT(*) FROM foo', level='none'), d_("{'results': [{'values': [[29]], 'types': ['integer'], 'columns': ['COUNT(*)']}]}"))

  def tearDown(self):
    self.cluster.deprovision()

if __name__ == "__main__":
  unittest.main(verbosity=2)
