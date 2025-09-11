#!/usr/bin/env python
#
# To run a specific test, execute
#
#  python system_test/full_system_test.py Class.test

import os
import unittest
import time

from helpers import Node, Cluster, d_, deprovision_node
from cdc_test_server import HTTPTestServer

RQLITED_PATH = os.environ['RQLITED_PATH']

class TestSingleNode_CDC(unittest.TestCase):
  def test_single_event(self):
    server = HTTPTestServer()
    server.start()
    url = server.url()

    n = Node(RQLITED_PATH, '0', cdc_config=url)
    n.start()
    n.wait_for_leader()
    j = n.execute('CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = n.execute('INSERT INTO bar(name) VALUES("fiona")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))

    server.wait_message_count(1)

    deprovision_node(n)

class TestMultiNode_CDC(unittest.TestCase):
  def test_multi_node_events(self):
    '''Test that a 3-node cluster sends the right number of CDC events'''
    server = HTTPTestServer()
    server.start()
    url = server.url()

    # Create 3-node cluster with CDC config
    n0 = Node(RQLITED_PATH, '0', cdc_config=url)
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1', cdc_config=url)
    n1.start(join=n0.RaftAddr())
    n1.wait_for_leader()

    n2 = Node(RQLITED_PATH, '2', cdc_config=url)
    n2.start(join=n0.RaftAddr())
    n2.wait_for_leader()

    cluster = Cluster([n0, n1, n2])
    leader = cluster.wait_for_leader()

    # Send some events
    j = leader.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = leader.execute('INSERT INTO foo(name) VALUES("alice")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = leader.execute('INSERT INTO foo(name) VALUES("bob")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 2, 'rows_affected': 1}]}"))

    # Ensure just the right number of events are sent (2: 2 INSERTs)
    server.wait_message_count(2)

    # Clean up
    cluster.deprovision()
    server.close()

  def test_multi_node_events_with_node_failure(self):
    '''Test that CDC continues working after killing one node'''
    server = HTTPTestServer()
    server.start()
    url = server.url()

    # Create 3-node cluster with CDC config
    n0 = Node(RQLITED_PATH, '0', cdc_config=url)
    n0.start()
    n0.wait_for_leader()

    n1 = Node(RQLITED_PATH, '1', cdc_config=url)
    n1.start(join=n0.RaftAddr())
    n1.wait_for_leader()

    n2 = Node(RQLITED_PATH, '2', cdc_config=url)
    n2.start(join=n0.RaftAddr())
    n2.wait_for_leader()

    cluster = Cluster([n0, n1, n2])
    leader = cluster.wait_for_leader()

    # Send some events
    j = leader.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = leader.execute('INSERT INTO foo(name) VALUES("alice")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = leader.execute('INSERT INTO foo(name) VALUES("bob")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 2, 'rows_affected': 1}]}"))

    # Wait for initial events
    server.wait_message_count(2)

    # Stop the current leader
    leader.stop()
    
    # Wait for a new leader to be elected (excluding the stopped one)
    new_leader = cluster.wait_for_leader(node_exc=leader)

    # Send a 4th event
    j = new_leader.execute('INSERT INTO foo(name) VALUES("charlie")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 3, 'rows_affected': 1}]}"))

    # Ensure just that event is sent
    server.wait_message_count(3)

    # Clean up
    cluster.deprovision()
    server.close()

if __name__ == "__main__":
  unittest.main(verbosity=2)
