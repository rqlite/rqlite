#!/usr/bin/env python
#
# End-to-end testing using actual rqlited binary.
#
# To run a specific test, execute
#
#  python system_test/full_system_test.py Class.test

import os
import unittest

from helpers import Node, Cluster, d_, write_random_file, deprovision_node, random_string, TIMEOUT

RQLITED_PATH = os.environ['RQLITED_PATH']

class TestBootstrapping(unittest.TestCase):
  '''Test simple bootstrapping works via -bootstrap-expect'''
  def test(self):
    n0 = Node(RQLITED_PATH, '0', bootstrap_expect=3)
    n1 = Node(RQLITED_PATH, '1', bootstrap_expect=3)
    n2 = Node(RQLITED_PATH, '2', bootstrap_expect=3)

    n0.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))
    n1.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))
    n2.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))

    self.assertEqual(n0.wait_for_leader(), n1.wait_for_leader())
    self.assertEqual(n0.wait_for_leader(), n2.wait_for_leader())

    # Ensure a 4th node can join later, with same launch params.
    n3 = Node(RQLITED_PATH, '4', bootstrap_expect=3)
    n3.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))

    n3.wait_for_leader()

    self.assertEqual(n3.wait_for_leader(), n0.wait_for_leader())

    deprovision_node(n0)
    deprovision_node(n1)
    deprovision_node(n2)
    deprovision_node(n3)

class TestBootstrappingRestart(unittest.TestCase):
  '''Test restarting a bootstrapped cluster works via -bootstrap-expect'''
  def test(self):
    n0 = Node(RQLITED_PATH, '0', bootstrap_expect=3)
    n1 = Node(RQLITED_PATH, '1', bootstrap_expect=3)
    n2 = Node(RQLITED_PATH, '2', bootstrap_expect=3)

    n0.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))
    n1.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))
    n2.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))

    self.assertEqual(n0.wait_for_leader(), n1.wait_for_leader())
    self.assertEqual(n0.wait_for_leader(), n2.wait_for_leader())

    # Restart all nodes, and ensure they still form a cluster using the same launch params.
    n0.stop()
    n1.stop()
    n2.stop()

    n0.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))
    n1.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))
    n2.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))
    self.assertEqual(n0.wait_for_leader(), n1.wait_for_leader())
    self.assertEqual(n0.wait_for_leader(), n2.wait_for_leader())

    deprovision_node(n0)
    deprovision_node(n1)
    deprovision_node(n2)

class TestBootstrappingRestartLeaveOnRemove(unittest.TestCase):
  '''Test restarting a bootstrapped cluster works via -bootstrap-expect when nodes self-remove'''
  def test(self):
    n0 = Node(RQLITED_PATH, '0', bootstrap_expect=3, raft_cluster_remove_shutdown=True)
    n1 = Node(RQLITED_PATH, '1', bootstrap_expect=3, raft_cluster_remove_shutdown=True)
    n2 = Node(RQLITED_PATH, '2', bootstrap_expect=3, raft_cluster_remove_shutdown=True)

    n0.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))
    n1.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))
    n2.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))

    self.assertEqual(n0.wait_for_leader(), n1.wait_for_leader())
    self.assertEqual(n0.wait_for_leader(), n2.wait_for_leader())

    # Restart one node, and ensure it still forms a cluster using the same launch params,
    # even though it was removed from the cluster on shutdown.
    n2.stop(graceful=True)
    self.assertEqual(len(n0.nodes()), 2)

    n2.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))
    n2.wait_for_leader() # Be sure node has joined cluster.

    # Ensure all nodes agree on which node is leader.
    self.assertEqual(n0.wait_for_leader(), n1.wait_for_leader())
    self.assertEqual(n0.wait_for_leader(), n2.wait_for_leader())
    self.assertEqual(len(n0.nodes()), 3)

    deprovision_node(n0)
    deprovision_node(n1)
    deprovision_node(n2)

class TestAutoClustering(unittest.TestCase):
  DiscoModeConsulKV = "consul-kv"
  DiscoModeEtcdKV = "etcd-kv"

  def autocluster(self, mode):
    disco_key = random_string(10)

    n0 = Node(RQLITED_PATH, '0')
    n0.start(disco_mode=mode, disco_key=disco_key)
    n0.wait_for_leader()
    self.assertEqual(n0.disco_mode(), mode)

    j = n0.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    n0.wait_for_all_fsm()
    j = n0.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    # Add second node, make sure it joins the cluster fine.
    n1 = Node(RQLITED_PATH, '1')
    n1.start(disco_mode=mode, disco_key=disco_key)
    n1.wait_for_leader()
    self.assertEqual(n1.disco_mode(), mode)
    j = n1.query('SELECT * FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    # Now a third.
    n2 = Node(RQLITED_PATH, '2')
    n2.start(disco_mode=mode, disco_key=disco_key)
    n2.wait_for_leader()
    self.assertEqual(n2.disco_mode(), mode)
    j = n2.query('SELECT * FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    # Now, kill the leader, which should trigger a different node to report leadership.
    deprovision_node(n0)

    # Add a fourth node, it should join fine using updated leadership details.
    # Use quick retries, as we know the leader information may be changing while
    # the node is coming up.
    n3 = Node(RQLITED_PATH, '3')
    n3.start(disco_mode=mode, disco_key=disco_key, join_interval='1s', join_attempts=1)
    n3.wait_for_leader()
    self.assertEqual(n3.disco_mode(), mode)
    j = n3.query('SELECT * FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    deprovision_node(n1)
    deprovision_node(n2)
    deprovision_node(n3)

  def autocluster_config(self, mode, config):
    disco_key = random_string(10)

    n0 = Node(RQLITED_PATH, '0')
    n0.start(disco_mode=mode, disco_key=disco_key, disco_config=config)
    n0.wait_for_leader()
    self.assertEqual(n0.disco_mode(), mode)

    j = n0.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    n0.wait_for_all_fsm()
    j = n0.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    # Add second node, make sure it joins the cluster fine.
    n1 = Node(RQLITED_PATH, '1')
    n1.start(disco_mode=mode, disco_key=disco_key, disco_config=config)
    n1.wait_for_leader()
    self.assertEqual(n1.disco_mode(), mode)
    j = n1.query('SELECT * FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    deprovision_node(n0)
    deprovision_node(n1)

  def test_consul(self):
    '''Test clustering via Consul and that leadership change is observed'''
    self.autocluster(TestAutoClustering.DiscoModeConsulKV)

  def test_etcd(self):
    '''Test clustering via Etcd and that leadership change is observed'''
    self.autocluster(TestAutoClustering.DiscoModeEtcdKV)

  def test_consul_config(self):
    '''Test clustering via Consul with explicit file-based config'''
    filename = write_random_file('{"address": "localhost:8500"}')
    self.autocluster_config(TestAutoClustering.DiscoModeConsulKV, filename)
    os.remove(filename)

  def test_etcd_config(self):
    '''Test clustering via Etcd with explicit file-based config'''
    filename = write_random_file('{"endpoints": ["localhost:2379"]}')
    self.autocluster_config(TestAutoClustering.DiscoModeEtcdKV, filename)
    os.remove(filename)

if __name__ == "__main__":
  unittest.main(verbosity=2)
