#!/usr/bin/env python

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
from helpers import Node, Cluster, d_, write_random_file, deprovision_node, is_sequence_number, random_string, TIMEOUT

RQLITED_PATH = os.environ['RQLITED_PATH']

class TestEndToEnd(unittest.TestCase):
  def setUp(self):
    n0 = Node(RQLITED_PATH, '0')
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1')
    n1.start(join=n0.APIAddr())
    n1.wait_for_leader()

    n2 = Node(RQLITED_PATH, '2')
    n2.start(join=n0.APIAddr())
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
    # Guard against any error in testing, by confirming that restarting the cluster
    # actually resulted in all new rqlited processes.
    self.assertTrue(pids.isdisjoint(set(self.cluster.pids())))

  def test_execute_fail_rejoin(self):
    '''Test that a node that fails can rejoin the cluster, and picks up changes'''

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

class TestEndToEndOnDisk(TestEndToEnd):
  def setUp(self):
    n0 = Node(RQLITED_PATH, '0', on_disk=True)
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1', on_disk=True)
    n1.start(join=n0.APIAddr())
    n1.wait_for_leader()

    n2 = Node(RQLITED_PATH, '2', on_disk=True)
    n2.start(join=n0.APIAddr())
    n2.wait_for_leader()

    self.cluster = Cluster([n0, n1, n2])

class TestEndToEndEncryptedNode(TestEndToEnd):
  def setUp(self):
    certFile = write_random_file(x509cert)
    keyFile = write_random_file(x509key)

    n0 = Node(RQLITED_PATH, '0', node_cert=certFile, node_key=keyFile, node_no_verify=True)
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1', node_cert=certFile, node_key=keyFile, node_no_verify=True)
    n1.start(join=n0.APIAddr())
    n1.wait_for_leader()

    n2 = Node(RQLITED_PATH, '2', node_cert=certFile, node_key=keyFile, node_no_verify=True)
    n2.start(join=n0.APIAddr())
    n2.wait_for_leader()

    self.cluster = Cluster([n0, n1, n2])

class TestJoinEncryptedNoVerify(unittest.TestCase):
  def test(self):
    ''' Test that a joining node will not operate if remote cert can't be trusted'''
    certFile = write_random_file(x509cert)
    keyFile = write_random_file(x509key)

    n0 = Node(RQLITED_PATH, '0', node_cert=certFile, node_key=keyFile, node_no_verify=False)
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1', node_cert=certFile, node_key=keyFile, node_no_verify=False)
    n1.start(join=n0.APIAddr())
    self.assertRaises(Exception, n1.wait_for_leader) # Join should fail due to bad cert.

    deprovision_node(n0)
    deprovision_node(n1)
    
class TestEndToEndAdvAddr(TestEndToEnd):
  def setUp(self):
    n0 = Node(RQLITED_PATH, '0',
              api_addr="0.0.0.0:4001", api_adv="localhost:4001",
              raft_addr="0.0.0.0:4002", raft_adv="localhost:4002")
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1')
    n1.start(join=n0.APIAddr())
    n1.wait_for_leader()

    n2 = Node(RQLITED_PATH, '2')
    n2.start(join=n0.APIAddr())
    n2.wait_for_leader()

    self.cluster = Cluster([n0, n1, n2])

class TestEndToEndAdvAddrEncryptedNode(TestEndToEnd):
  def setUp(self):
    certFile = write_random_file(x509cert)
    keyFile = write_random_file(x509key)

    n0 = Node(RQLITED_PATH, '0',
              api_addr="0.0.0.0:4001", api_adv="localhost:4001",
              raft_addr="0.0.0.0:4002", raft_adv="localhost:4002",
               node_cert=certFile, node_key=keyFile, node_no_verify=True)
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1', node_cert=certFile, node_key=keyFile, node_no_verify=True)
    n1.start(join=n0.APIAddr())
    n1.wait_for_leader()

    n2 = Node(RQLITED_PATH, '2', node_cert=certFile, node_key=keyFile, node_no_verify=True)
    n2.start(join=n0.APIAddr())
    n2.wait_for_leader()

    self.cluster = Cluster([n0, n1, n2])

class TestBootstrapping(unittest.TestCase):
  '''Test simple bootstrapping works via -bootstrap-expect'''
  def test(self):
    n0 = Node(RQLITED_PATH, '0', boostrap_expect=3)
    n1 = Node(RQLITED_PATH, '1', boostrap_expect=3)
    n2 = Node(RQLITED_PATH, '2', boostrap_expect=3)

    n0.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))
    n1.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))
    n2.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))

    self.assertEqual(n0.wait_for_leader(), n1.wait_for_leader())
    self.assertEqual(n0.wait_for_leader(), n2.wait_for_leader())

    # Ensure a 4th node can join later, with same launch params.
    n3 = Node(RQLITED_PATH, '4', boostrap_expect=3)
    n3.start(join=','.join([n0.APIProtoAddr(), n1.APIProtoAddr(), n2.APIProtoAddr()]))

    n3.wait_for_leader()

    self.assertEqual(n3.wait_for_leader(), n0.wait_for_leader())

    deprovision_node(n0)
    deprovision_node(n1)
    deprovision_node(n2)
    deprovision_node(n3)

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

class TestAuthJoin(unittest.TestCase):
  '''Test that joining works with authentication'''

  def test(self):
    self.auth_file = tempfile.NamedTemporaryFile()
    with open(self.auth_file.name, 'w') as f:
      f.write('[{"username": "foo","password": "bar","perms": ["all"]}, {"username": "*", "perms": ["status", "ready"]}]')

    n0 = Node(RQLITED_PATH, '0', auth=self.auth_file.name)
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1', auth=self.auth_file.name)
    n1.start(join=n0.APIAddr())
    self.assertTrue(n1.expect_leader_fail()) # Join should fail due to lack of auth.

    n2 = Node(RQLITED_PATH, '2', auth=self.auth_file.name)
    n2.start(join=n0.APIAddr(), join_as="foo")
    n2.wait_for_leader()

    self.cluster = Cluster([n0, n1, n2])

  def tearDown(self):
    self.auth_file.close()
    self.cluster.deprovision()

class TestClusterRecovery(unittest.TestCase):
  '''Test that a cluster can recover after all Raft network addresses change'''
  def test(self):
    n0 = Node(RQLITED_PATH, '0')
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1')
    n1.start(join=n0.APIAddr())
    n1.wait_for_leader()

    n2 = Node(RQLITED_PATH, '2')
    n2.start(join=n0.APIAddr())
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
    n1.start(join=n0.APIAddr())
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
      self.assertEqual(j, d_("{'results': [{'columns': ['COUNT(*)'], 'types': [''], 'values': [[2002]]}]}"))

class TestEndToEndNonVoter(unittest.TestCase):
  def setUp(self):
    self.leader = Node(RQLITED_PATH, '0')
    self.leader.start()
    self.leader.wait_for_leader()

    self.non_voter = Node(RQLITED_PATH, '1', raft_voter=False)
    self.non_voter.start(join=self.leader.APIAddr())
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
    return
    '''Test that non-voters supply the correct leader redirects (HTTP 301)'''

    l = self.cluster.wait_for_leader()
    fs = self.cluster.followers()
    self.assertEqual(len(fs), 1)
    for n in fs:
      self.assertEqual(l.APIAddr(), n.redirect_addr())

    l.stop()
    n = self.cluster.wait_for_leader(node_exc=l)
    for f in self.cluster.followers():
      self.assertEqual(n.APIAddr(), f.redirect_addr())

class TestEndToEndNonVoterFollowsLeader(unittest.TestCase):
  def setUp(self):
    n0 = Node(RQLITED_PATH, '0')
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1')
    n1.start(join=n0.APIAddr())
    n1.wait_for_leader()

    n2 = Node(RQLITED_PATH, '2')
    n2.start(join=n0.APIAddr())
    n2.wait_for_leader()

    self.non_voter = Node(RQLITED_PATH, '3', raft_voter=False)
    self.non_voter.start(join=n0.APIAddr())
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
    self.node1.start(join=self.node0.APIAddr())
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
    self.node4.start(join=self.node3.APIAddr())
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
  def waitForSnap(self, n):
    timeout = 10
    t = 0
    while True:
      if t > timeout:
        raise Exception('timeout')
      if self.n0.num_snapshots() is n:
        break
      time.sleep(1)
      t+=1

  def test_join_with_snap(self):
    '''Check that a node joins a cluster correctly via a snapshot'''
    self.n0 = Node(RQLITED_PATH, '0',  raft_snap_threshold=100, raft_snap_int="1s")
    self.n0.start()
    self.n0.wait_for_leader()

    # Let's get TWO snapshots done.
    self.n0.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    for i in range(0,100):
      self.n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.n0.wait_for_all_fsm()
    self.waitForSnap(1)

    for i in range(0,100):
      self.n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.n0.wait_for_all_fsm()
    self.waitForSnap(2)

    # Add two more nodes to the cluster
    self.n1 = Node(RQLITED_PATH, '1')
    self.n1.start(join=self.n0.APIAddr())
    self.n1.wait_for_leader()

    self.n2 = Node(RQLITED_PATH, '2')
    self.n2.start(join=self.n0.APIAddr())
    self.n2.wait_for_leader()

    # Force the Apply loop to run on the node, so fsm_index is updated.
    self.n0.execute('INSERT INTO foo(name) VALUES("fiona")')

    # Ensure those new nodes have the full correct state.
    self.n1.wait_for_fsm_index(self.n0.fsm_index())
    j = self.n1.query('SELECT count(*) FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[201]], 'types': [''], 'columns': ['count(*)']}]}"))

    self.n2.wait_for_fsm_index(self.n0.fsm_index())
    j = self.n2.query('SELECT count(*) FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[201]], 'types': [''], 'columns': ['count(*)']}]}"))

    # Kill one of the nodes, and make more changes, enough to trigger more snaps.
    self.n2.stop()

    for i in range(0,100):
      self.n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.n0.wait_for_all_fsm()
    self.waitForSnap(3)
    for i in range(0,100):
      self.n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.n0.wait_for_all_fsm()
    self.waitForSnap(4)

    # Restart killed node, check it has full state.
    self.n2.start()
    self.n2.wait_for_leader()

    # Force the Apply loop to run on the node twice, so fsm_index is updated on n2 to a point
    # where the SQLite database on n2 is updated.
    self.n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.n2.query('SELECT count(*) FROM foo', level='strong')

    # Snapshot testing is tricky, as it's so timing-based -- and therefore hard to predict.
    # So adopt a polling approach.
    t = 0
    while True:
      if t > TIMEOUT:
        raise Exception('timeout waiting for n2 to have all data')
      j = self.n2.query('SELECT count(*) FROM foo', level='none')
      if j == d_("{'results': [{'values': [[402]], 'types': [''], 'columns': ['count(*)']}]}"):
        break
      time.sleep(1)
      t+=1

  def tearDown(self):
    deprovision_node(self.n0)
    deprovision_node(self.n1)
    deprovision_node(self.n2)

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
    self.n1.start(join=self.n0.APIAddr())
    self.n1.wait_for_leader()

    self.assertEqual(self.n0.num_join_requests(), 1)

    self.n1.stop()
    self.n1.start(join=self.n0.APIAddr())
    self.n1.wait_for_leader()
    self.assertEqual(self.n0.num_join_requests(), 2)

class TestJoinCatchup(unittest.TestCase):
  def setUp(self):
    self.n0 = Node(RQLITED_PATH, '0')
    self.n0.start()
    self.n0.wait_for_leader()

    self.n1 = Node(RQLITED_PATH, '1')
    self.n1.start(join=self.n0.APIAddr())
    self.n1.wait_for_leader()

    self.n2 = Node(RQLITED_PATH, '2')
    self.n2.start(join=self.n0.APIAddr())
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
    applied = n0.wait_for_all_fsm()

    # Test that follower node has correct state in local database, and then kill the follower
    self.n1.wait_for_fsm_index(applied)
    j = self.n1.query('SELECT * FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))
    self.n1.stop()

    # Insert a new record
    j = n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 2, 'rows_affected': 1}]}"))
    j = n0.query('SELECT COUNT(*) FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[2]], 'types': [''], 'columns': ['COUNT(*)']}]}"))
    applied = n0.wait_for_all_fsm()

    # Restart follower, explicity rejoin, and ensure it picks up new records
    self.n1.start(join=self.n0.APIAddr())
    self.n1.wait_for_leader()
    self.n1.wait_for_fsm_index(applied)
    self.assertEqual(n0.expvar()['store']['num_ignored_joins'], 1)
    j = self.n1.query('SELECT COUNT(*) FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[2]], 'types': [''], 'columns': ['COUNT(*)']}]}"))

  def test_change_addresses(self):
    '''Test that a node rejoining with new addresses works fine'''
    n0 = self.cluster.wait_for_leader()
    j = n0.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = n0.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))
    applied = n0.wait_for_all_fsm()

    # Test that follower node has correct state in local database, and then kill the follower
    self.n1.wait_for_fsm_index(applied)
    j = self.n1.query('SELECT * FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))
    self.n1.stop()

    # Insert a new record
    j = n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 2, 'rows_affected': 1}]}"))
    j = n0.query('SELECT COUNT(*) FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[2]], 'types': [''], 'columns': ['COUNT(*)']}]}"))
    applied = n0.wait_for_all_fsm()

    # Restart follower with new network attributes, explicity rejoin, and ensure it picks up new records
    self.n1.scramble_network()
    self.n1.start(join=self.n0.APIAddr())
    self.n1.wait_for_leader()
    self.assertEqual(n0.expvar()['store']['num_removed_before_joins'], 1)
    self.n1.wait_for_fsm_index(applied)
    j = self.n1.query('SELECT COUNT(*) FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[2]], 'types': [''], 'columns': ['COUNT(*)']}]}"))

class TestRedirectedJoin(unittest.TestCase):
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
    self.n1.start(join=self.n0.APIAddr())
    self.n1.wait_for_leader()
    self.assertTrue(self.n1.is_follower())

    self.n2 = Node(RQLITED_PATH, '2')
    self.n2.start(join=self.n1.APIAddr())
    l2 = self.n2.wait_for_leader()
    self.assertEqual(l0, l2)

  def test_api_adv(self):
    '''Test that a node can join via a follower that advertises a different API address'''
    self.n0 = Node(RQLITED_PATH, '0',
      api_addr="0.0.0.0:4001", api_adv="localhost:4001")
    self.n0.start()
    l0 = self.n0.wait_for_leader()

    self.n1 = Node(RQLITED_PATH, '1')
    self.n1.start(join=self.n0.APIAddr())
    self.n1.wait_for_leader()
    self.assertTrue(self.n1.is_follower())

    self.n2 = Node(RQLITED_PATH, '2')
    self.n2.start(join=self.n1.APIAddr())
    l2 = self.n2.wait_for_leader()
    self.assertEqual(l0, l2)

if __name__ == "__main__":
  unittest.main(verbosity=2)

