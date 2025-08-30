#!/usr/bin/env python
#
# To run a specific test, execute
#
#  python system_test/full_system_test.py Class.test

import os
import unittest
import time

from helpers import Node, d_, deprovision_node
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

    server.wait_message_count(2)

    deprovision_node(n)

if __name__ == "__main__":
  unittest.main(verbosity=2)
