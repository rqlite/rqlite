#!/usr/bin/env python
# End-to-end tests using the real rqlited binary and HTTP over Unix sockets.
# RQLITED_PATH=/path/to/rqlited python3 system_test/e2e/unix_socket.py

from contextlib import closing
import http.client
import json
import os
import socket
import tempfile
import time
import unittest

from helpers import Node, deprovision_node

RQLITED_PATH = os.environ['RQLITED_PATH']


class UnixHTTPConnection(http.client.HTTPConnection):
  def __init__(self, path):
    super().__init__('localhost', timeout=5)
    self.path = path

  def connect(self):
    self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    self.sock.settimeout(self.timeout)
    self.sock.connect(self.path)


@unittest.skipUnless(os.name == 'posix' and hasattr(socket, 'AF_UNIX'),
                     'requires Unix domain sockets')
class TestUnixSocket(unittest.TestCase):
  def setUp(self):
    self.sockets = tempfile.TemporaryDirectory(prefix='rqlite-unix-')
    self.addCleanup(self.sockets.cleanup)
    self.path = os.path.join(self.sockets.name, 'http.sock')
    self.node = Node(RQLITED_PATH, 'leader', http_unix_socket=self.path)
    self.addCleanup(deprovision_node, self.node)
    self.node.start()
    self.node.wait_for_leader()

  def request(self, method, endpoint, statements=None, path=None):
    with closing(UnixHTTPConnection(path or self.path)) as conn:
      body = None if statements is None else json.dumps(statements)
      conn.request(method, endpoint, body, {'Content-Type': 'application/json'})
      resp = conn.getresponse()
      self.assertEqual(resp.status, 200)
      return json.loads(resp.read())

  def test_read_write_and_restart(self):
    self.request('POST', '/db/execute', ['CREATE TABLE foo (id INTEGER PRIMARY KEY)'])
    self.request('POST', '/db/execute', ['INSERT INTO foo VALUES(1)'])
    self.assertEqual(self.request('GET', '/db/query?level=none&q=SELECT%20id%20FROM%20foo'),
                     self.node.query('SELECT id FROM foo'))
    status = self.request('GET', '/status')
    self.assertEqual(status['http']['bind_addr'], self.node.api_addr)
    self.assertNotIn(self.path, json.dumps(self.request('GET', '/nodes')))

    self.node.stop(graceful=True)
    self.assertFalse(os.path.exists(self.path))
    self.node.start()
    self.node.wait_for_leader()
    rows = self.request('GET', '/db/query?level=none&q=SELECT%20id%20FROM%20foo')
    self.assertEqual(rows['results'][0]['values'], [[1]])

  def test_follower_forwarding(self):
    path = os.path.join(self.sockets.name, 'follower.sock')
    follower = Node(RQLITED_PATH, 'follower', raft_voter=False, http_unix_socket=path)
    self.addCleanup(deprovision_node, follower)
    follower.start(join=self.node.RaftAddr())
    follower.wait_for_leader()

    # Both the write and weak read go through the existing cluster RPC path.
    self.request('POST', '/db/execute', ['CREATE TABLE foo (id INTEGER PRIMARY KEY)'], path)
    self.request('POST', '/db/execute', ['INSERT INTO foo VALUES(1)'], path)
    rows = self.request('GET', '/db/query?level=weak&q=SELECT%20id%20FROM%20foo', path=path)
    self.assertEqual(rows['results'][0]['values'], [[1]])

    # Once replication catches up, the follower can also read its local copy.
    deadline = time.time() + 10
    while time.time() < deadline:
      rows = self.request('GET', '/db/query?level=none&q=SELECT%20id%20FROM%20foo', path=path)
      if rows['results'][0].get('values') == [[1]]:
        break
      time.sleep(0.1)
    else:
      self.fail('follower did not apply the replicated write')
    nodes = self.request('GET', '/nodes', path=path)
    self.assertNotIn(path, json.dumps(nodes))
    self.assertNotIn(self.path, json.dumps(nodes))


if __name__ == '__main__':
  unittest.main()
