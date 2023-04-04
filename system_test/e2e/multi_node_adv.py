#!/usr/bin/env python
#
# End-to-end testing using actual rqlited binary.
#
# To run a specific test, execute
#
#  python system_test/full_system_test.py Class.test

import os
import unittest

from certs import x509cert, x509key
from helpers import Node, Cluster, write_random_file
from multi_node import TestEndToEnd

RQLITED_PATH = os.environ['RQLITED_PATH']
    
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

if __name__ == "__main__":
  unittest.main(verbosity=2)

