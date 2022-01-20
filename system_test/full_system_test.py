#!/usr/bin/env python

# End-to-end testing using actual rqlited binary.
#
# To run a specific test, execute
#
#  python system_test/full_system_test.py Class.test

import tempfile
import argparse
import ast
import subprocess
import requests
import json
import os
import random
import shutil
import time
import socket
import sqlite3
import string
import sys
import unittest

from urllib.parse import urlparse

RQLITED_PATH = os.environ['RQLITED_PATH']
TIMEOUT=10

def d_(s):
    return ast.literal_eval(s.replace("'", "\""))

def random_string(n):
  letters = string.ascii_lowercase
  return ''.join(random.choice(letters) for i in range(n))

def write_random_file(data):
  f = tempfile.NamedTemporaryFile('w', delete=False)
  f.write(data)
  f.close()
  return f.name

class Node(object):
  def __init__(self, path, node_id,
               api_addr=None, api_adv=None,
               raft_addr=None, raft_adv=None,
               raft_voter=True,
               raft_snap_threshold=8192, raft_snap_int="1s",
               auth=None, dir=None, on_disk=False):
    if api_addr is None:
      s, addr = random_addr()
      api_addr = addr
      s.close()
    if raft_addr is None:
      s, addr = random_addr()
      raft_addr = addr
      s.close()
    if dir is None:
      dir = tempfile.mkdtemp()
    if api_adv is None:
      api_adv = api_addr

    self.dir = dir
    self.path = path
    self.peers_path = os.path.join(self.dir, "raft/peers.json")
    self.node_id = node_id
    self.api_addr = api_addr
    self.api_adv = api_adv
    self.raft_addr = raft_addr
    self.raft_adv = raft_adv
    self.raft_voter = raft_voter
    self.raft_snap_threshold = raft_snap_threshold
    self.raft_snap_int = raft_snap_int
    self.auth = auth
    self.disco_key = random_string(10)
    self.on_disk = on_disk
    self.process = None
    self.stdout_file = os.path.join(dir, 'rqlited.log')
    self.stdout_fd = open(self.stdout_file, 'w')
    self.stderr_file = os.path.join(dir, 'rqlited.err')
    self.stderr_fd = open(self.stderr_file, 'w')

  def APIAddr(self):
      if self.api_adv is not None:
          return self.api_adv
      return self.api_addr

  def APIProtoAddr(self):
      return "http://%s" % self.APIAddr()

  def scramble_network(self):
    if self.api_adv == self.api_addr:
      self.api_adv = None

    s, addr = random_addr()
    self.api_addr = addr
    s.close()

    if self.api_adv is None:
      self.api_adv = self.api_addr

    if self.raft_adv == self.raft_addr:
      self.raft_adv = None

    s, addr = random_addr()
    self.raft_addr = addr
    s.close()

    if self.raft_adv is None:
      self.raft_adv = self.raft_addr

  def start(self, join=None, join_as=None, join_attempts=None, join_interval=None,
    disco_mode=None, disco_key=None, disco_config=None, wait=True, timeout=TIMEOUT):
    if self.process is not None:
      return

    command = [self.path,
               '-node-id', self.node_id,
               '-http-addr', self.api_addr,
               '-raft-addr', self.raft_addr,
               '-raft-snap', str(self.raft_snap_threshold),
               '-raft-snap-int', self.raft_snap_int,
               '-raft-non-voter=%s' % str(not self.raft_voter).lower()]
    if self.api_adv is not None:
      command += ['-http-adv-addr', self.api_adv]
    if self.raft_adv is not None:
      command += ['-raft-adv-addr', self.raft_adv]
    if self.on_disk:
      command += ['-on-disk']
    if self.auth is not None:
      command += ['-auth', self.auth]
    if join is not None:
      command += ['-join', 'http://' + join]
    if join_as is not None:
       command += ['-join-as', join_as]
    if join_attempts is not None:
       command += ['-join-attempts', str(join_attempts)]
    if join_interval is not None:
       command += ['-join-interval', join_interval]
    if disco_mode is not None:
      dk = disco_key
      if dk is None:
        dk = self.disco_key
      command += ['-disco-mode', disco_mode, '-disco-key', dk]
      if disco_config is not None:
        command += ['-disco-config', disco_config]
    command.append(self.dir)

    self.process = subprocess.Popen(command, stdout=self.stdout_fd, stderr=self.stderr_fd)
    t = 0
    while wait:
      if t > timeout:
        raise Exception('timeout')
      try:
        self.status()
      except requests.exceptions.ConnectionError:
        time.sleep(1)
        t+=1
      else:
        break
    return self

  def stop(self):
    if self.process is None:
      return
    self.process.kill()
    self.process.wait()
    self.process = None
    return self

  def pid(self):
    if self.process is None:
      return None
    return self.process.pid

  def status(self):
    r = requests.get(self._status_url())
    raise_for_status(r)
    return r.json()

  def nodes(self):
    r = requests.get(self._nodes_url())
    raise_for_status(r)
    return r.json()

  def ready(self):
    r = requests.get(self._ready_url())
    return r.status_code == 200

  def expvar(self):
    r = requests.get(self._expvar_url())
    raise_for_status(r)
    return r.json()

  def is_leader(self):
    '''
    is_leader returns whether this node is the cluster leader
    It also performs a check, to ensure the node nevers gives out
    conflicting information about leader state.
    '''

    try:
      return self.status()['store']['raft']['state'] == 'Leader'
    except requests.exceptions.ConnectionError:
      return False

  def is_follower(self):
    try:
      return self.status()['store']['raft']['state'] == 'Follower'
    except requests.exceptions.ConnectionError:
      return False

  def disco_mode(self):
    try:
      return self.status()['disco']['name']
    except requests.exceptions.ConnectionError:
      return ''

  def wait_for_leader(self, timeout=TIMEOUT):
    lr = None
    t = 0
    while lr == None or lr['addr'] == '':
      if t > timeout:
        raise Exception('timeout')
      try:
        lr = self.status()['store']['leader']
      except requests.exceptions.ConnectionError:
        pass
      time.sleep(1)
      t+=1

    # Perform a check on readyness while we're here.
    if self.ready() is not True:
      raise Exception('leader is available but node reports not ready')
    return lr

  def db_applied_index(self):
    return int(self.status()['store']['db_applied_index'])

  def fsm_index(self):
    return int(self.status()['store']['fsm_index'])

  def commit_index(self):
    return int(self.status()['store']['raft']['commit_index'])

  def applied_index(self):
    return int(self.status()['store']['raft']['applied_index'])

  def last_log_index(self):
    return int(self.status()['store']['raft']['last_log_index'])

  def last_snapshot_index(self):
    return int(self.status()['store']['raft']['last_snapshot_index'])

  def num_join_requests(self):
    return int(self.expvar()['http']['joins'])

  def wait_for_fsm_index(self, index, timeout=TIMEOUT):
    '''
    Wait until the given index has been applied to the state machine.
    '''
    t = 0
    while self.fsm_index() < index:
      if t > timeout:
        raise Exception('timeout, target index: %d, actual index %d' % (index, self.fsm_index()))
      time.sleep(1)
      t+=1
    return self.fsm_index()

  def wait_for_commit_index(self, index, timeout=TIMEOUT):
    '''
    Wait until the commit index reaches the given value
    '''
    t = 0
    while self.commit_index() < index:
      if t > timeout:
        raise Exception('timeout')
      time.sleep(1)
      t+=1
    return self.commit_index()

  def wait_for_all_applied(self, timeout=TIMEOUT):
    '''
    Wait until the applied index equals the commit index.
    '''
    t = 0
    while self.commit_index() != self.applied_index():
      if t > timeout:
        raise Exception('timeout')
      time.sleep(1)
      t+=1
    return self.applied_index()

  def wait_for_all_fsm(self, timeout=TIMEOUT):
    '''
    Wait until all outstanding database commands have actually
    been applied to the database i.e. state machine.
    '''
    t = 0
    while self.fsm_index() != self.db_applied_index():
      if t > timeout:
        raise Exception('timeout')
      time.sleep(1)
      t+=1
    return self.fsm_index()

  def query(self, statement, params=None, level='weak', pretty=False, text=False):
    body = [statement]
    if params is not None:
      try:
        body = body + params
      except TypeError:
        # Presumably not a list, so append as an object.
        body.append(params)

    reqParams = {'level': level}
    if pretty:
      reqParams['pretty'] = "yes"
    r = requests.post(self._query_url(), params=reqParams, data=json.dumps([body]))
    raise_for_status(r)
    if text:
      return r.text
    return r.json()

  def execute(self, statement, params=None):
    body = [statement]
    if params is not None:
      try:
        body = body + params
      except TypeError:
        # Presumably not a list, so append as an object.
        body.append(params)
    return self.execute_raw(json.dumps([body]))

  def execute_raw(self, body):
    r = requests.post(self._execute_url(), data=body)
    raise_for_status(r)
    return r.json()

  def backup(self, file):
    with open(file, 'wb') as fd:
      r = requests.get(self._backup_url())
      raise_for_status(r)
      fd.write(r.content)

  def restore(self, file):
    # This is the one API that doesn't expect JSON.
    conn = sqlite3.connect(file)
    r = requests.post(self._load_url(), data='\n'.join(conn.iterdump()))
    raise_for_status(r)
    conn.close()
    return r.json()

  def redirect_addr(self):
    r = requests.post(self._execute_url(redirect=True), data=json.dumps(['nonsense']), allow_redirects=False)
    raise_for_status(r)
    if r.status_code == 301:
      return "%s://%s" % (urlparse(r.headers['Location']).scheme, urlparse(r.headers['Location']).netloc)

  def set_peers(self, peers):
    f = open(self.peers_path, "w")
    f.write(json.dumps(peers))
    f.close()

  def _status_url(self):
    return 'http://' + self.APIAddr() + '/status'
  def _nodes_url(self):
    return 'http://' + self.APIAddr() + '/nodes?nonvoters' # Getting all nodes back makes testing easier
  def _ready_url(self):
    return 'http://' + self.APIAddr() + '/readyz'
  def _expvar_url(self):
    return 'http://' + self.APIAddr() + '/debug/vars'
  def _query_url(self, redirect=False):
    rd = ""
    if redirect:
      rd = "?redirect"
    return 'http://' + self.APIAddr() + '/db/query' + rd
  def _execute_url(self, redirect=False):
    rd = ""
    if redirect:
      rd = "?redirect"
    return 'http://' + self.APIAddr() + '/db/execute' + rd
  def _backup_url(self):
    return 'http://' + self.APIAddr() + '/db/backup'
  def _load_url(self):
    return 'http://' + self.APIAddr() + '/db/load'
  def __eq__(self, other):
    return self.node_id == other.node_id
  def __str__(self):
    return '%s:[%s]:[%s]:[%s]' % (self.node_id, self.APIAddr(), self.raft_addr, self.dir)
  def __del__(self):
    self.stdout_fd.close()
    self.stderr_fd.close()

def raise_for_status(r):
  try:
    r.raise_for_status()
  except requests.exceptions.HTTPError as e:
    print(e)
    print((r.text))
    raise e

def random_addr():
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.bind(('localhost', 0))
  return s, ':'.join([s.getsockname()[0], str(s.getsockname()[1])])

def deprovision_node(node):
  node.stop()
  if os.path.isdir(node.dir):
    shutil.rmtree(node.dir)
  node = None

class Cluster(object):
  def __init__(self, nodes):
    self.nodes = nodes
  def wait_for_leader(self, node_exc=None, timeout=TIMEOUT):
    t = 0
    while True:
      if t > timeout:
        raise Exception('timeout')
      for n in self.nodes:
        if node_exc is not None and n == node_exc:
          continue
        if n.is_leader():
          return n
      time.sleep(1)
      t+=1
  def start(self):
    for n in self.nodes:
      n.start()
  def stop(self):
    for n in self.nodes:
      n.stop()
  def followers(self):
    return [n for n in self.nodes if n.is_follower()]
  def pids(self):
    '''
    Return a sorted list of all rqlited PIDs in the cluster.
    '''
    return sorted([n.pid() for n in self.nodes])
  def deprovision(self):
    for n in self.nodes:
      deprovision_node(n)

class TestSingleNode(unittest.TestCase):
  def setUp(self):
    n0 = Node(RQLITED_PATH, '0',  raft_snap_threshold=2, raft_snap_int="1s")
    n0.start()
    n0.wait_for_leader()

    self.cluster = Cluster([n0])

  def tearDown(self):
    self.cluster.deprovision()

  def test_simple_raw_queries(self):
    '''Test simple queries work as expected'''
    n = self.cluster.wait_for_leader()
    j = n.execute('CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = n.execute('INSERT INTO bar(name) VALUES("fiona")')
    applied = n.wait_for_all_fsm()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = n.query('SELECT * from bar')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    # Ensure raw response from API is as expected.
    j = n.query('SELECT * from bar', text=True)
    self.assertEqual(str(j), '{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"]]}]}')

  def test_simple_raw_queries_pretty(self):
    '''Test simple queries, requesting pretty output, work as expected'''
    n = self.cluster.wait_for_leader()
    j = n.execute('CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = n.execute('INSERT INTO bar(name) VALUES("fiona")')
    applied = n.wait_for_all_fsm()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = n.query('SELECT * from bar', pretty=True, text=True)
    exp = '''{
    "results": [
        {
            "columns": [
                "id",
                "name"
            ],
            "types": [
                "integer",
                "text"
            ],
            "values": [
                [
                    1,
                    "fiona"
                ]
            ]
        }
    ]
}'''
    self.assertEqual(str(j), exp)

  def test_simple_parameterized_queries(self):
    '''Test parameterized queries work as expected'''
    n = self.cluster.wait_for_leader()
    j = n.execute('CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = n.execute('INSERT INTO bar(name, age) VALUES(?,?)', params=["fiona", 20])
    applied = n.wait_for_all_fsm()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = n.query('SELECT * from bar')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona', 20]], 'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))
    j = n.query('SELECT * from bar WHERE age=?', params=[20])
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona', 20]], 'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))
    j = n.query('SELECT * from bar WHERE age=?', params=[21])
    self.assertEqual(j, d_("{'results': [{'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))

  def test_simple_named_parameterized_queries(self):
    '''Test named parameterized queries work as expected'''
    n = self.cluster.wait_for_leader()
    j = n.execute('CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = n.execute('INSERT INTO bar(name, age) VALUES(?,?)', params=["fiona", 20])
    applied = n.wait_for_all_fsm()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = n.query('SELECT * from bar')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona', 20]], 'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))
    j = n.query('SELECT * from bar WHERE age=:age', params={"age": 20})
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona', 20]], 'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))

  def test_simple_parameterized_mixed_queries(self):
    '''Test a mix of parameterized and non-parameterized queries work as expected'''
    n = self.cluster.wait_for_leader()
    j = n.execute('CREATE TABLE bar (id INTEGER NOT NULL PRIMARY KEY, name TEXT, age INTEGER)')
    self.assertEqual(j, d_("{'results': [{}]}"))

    body = json.dumps([
        ["INSERT INTO bar(name, age) VALUES(?,?)", "fiona", 20],
        ['INSERT INTO bar(name, age) VALUES("sinead", 25)']
    ])
    j = n.execute_raw(body)
    applied = n.wait_for_all_fsm()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}, {'last_insert_id': 2, 'rows_affected': 1}]}"))
    j = n.query('SELECT * from bar')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona', 20], [2, 'sinead', 25]], 'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))

  def test_snapshot(self):
    ''' Test that a node peforms at least 1 snapshot'''
    n = self.cluster.wait_for_leader()
    j = n.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.assertEqual(j, d_("{'results': [{}]}"))
    j = n.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))

    applied = n.wait_for_all_fsm()

    # Wait for a snapshot to happen.
    timeout = 10
    t = 0
    while True:
      nSnaps = n.expvar()['store']['num_snapshots']
      if nSnaps > 0:
        return
      if t > timeout:
        raise Exception('timeout', nSnaps)
      time.sleep(1)
      t+=1

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
    pids = self.cluster.pids()

    self.cluster.stop()
    self.cluster.start()
    self.cluster.wait_for_leader()
    # Guard against any error in testing, by confirming that restarting the cluster
    # actually resulted in new rqlite processes.
    self.assertNotEqual(pids, self.cluster.pids())

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

class TestAutoClustering(unittest.TestCase):
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
    self.autocluster('consul')

  def test_etcd(self):
    '''Test clustering via Etcd and that leadership change is observed'''
    self.autocluster('etcd')

  def test_consul_config(self):
    '''Test clustering via Consul with explicit file-based config'''
    filename = write_random_file('{"address": "localhost:8500"}')
    self.autocluster_config('consul', filename)
    os.remove(filename)

  def test_etcd_config(self):
    '''Test clustering via Etcd with explicit file-based config'''
    filename = write_random_file('{"endpoints": ["localhost:2379"]}')
    self.autocluster_config('etcd', filename)
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
    self.assertRaises(Exception, n1.wait_for_leader) # Join should fail due to lack of auth.

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

    self.node0 = Node(RQLITED_PATH, '0')
    self.node0.start()
    self.node0.wait_for_leader()
    self.node0.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
    self.node0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.node0.wait_for_all_fsm()

    self.node0.backup(self.db_file)
    conn = sqlite3.connect(self.db_file)
    rows = conn.execute('SELECT * FROM foo').fetchall()
    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0], (1, 'fiona'))
    conn.close()

    self.node1 = Node(RQLITED_PATH, '1')
    self.node1.start()
    self.node1.wait_for_leader()
    j = self.node1.restore(self.db_file)
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 1, 'rows_affected': 1}]}"))
    j = self.node1.query('SELECT * FROM foo')
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

  def tearDown(self):
    if hasattr(self, 'node0'):
      deprovision_node(self.node0)
    if hasattr(self, 'node1'):
      deprovision_node(self.node1)
    os.remove(self.db_file)

class TestEndToEndSnapRestoreSingle(unittest.TestCase):
  def setUp(self):
    self.n0 = Node(RQLITED_PATH, '0',  raft_snap_threshold=10, raft_snap_int="1s")
    self.n0.start()
    self.n0.wait_for_leader()

  def waitForSnapIndex(self, n):
    timeout = 10
    t = 0
    while True:
      if t > timeout:
        raise Exception('timeout')
      if self.n0.last_snapshot_index() >= n:
        break
      time.sleep(1)
      t+=1

  def test_snap_and_restart(self):
    '''Check that an node restarts correctly after multiple snapshots'''

    # Let's get multiple snapshots done.
    self.n0.execute('CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')

    for i in range(0,200):
      self.n0.execute('INSERT INTO foo(name) VALUES("fiona")')
    self.n0.wait_for_all_fsm()
    self.waitForSnapIndex(175)

    # Ensure node has the full correct state.
    j = self.n0.query('SELECT count(*) FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[200]], 'types': [''], 'columns': ['count(*)']}]}"))

    # Restart node, and make sure it comes back with the correct state
    self.n0.stop()
    self.n0.start()
    self.n0.wait_for_leader()
    self.n0.wait_for_all_applied()
    self.assertEqual(self.n0.expvar()['store']['num_restores'], 1)

    j = self.n0.query('SELECT count(*) FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[200]], 'types': [''], 'columns': ['count(*)']}]}"))

  def tearDown(self):
    deprovision_node(self.n0)

class TestEndToEndSnapRestoreSingleOnDisk(TestEndToEndSnapRestoreSingle):
  def setUp(self):
    self.n0 = Node(RQLITED_PATH, '0',  raft_snap_threshold=10, raft_snap_int="1s", on_disk=True)
    self.n0.start()
    self.n0.wait_for_leader()

class TestEndToEndSnapRestoreCluster(unittest.TestCase):
  def waitForSnap(self, n):
    timeout = 10
    t = 0
    while True:
      if t > timeout:
        raise Exception('timeout')
      if self.n0.expvar()['store']['num_snapshots'] is n:
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

    # Force the Apply loop to run on the node, so fsm_index is updated.
    self.n0.execute('INSERT INTO foo(name) VALUES("fiona")')

    self.n2.wait_for_fsm_index(self.n0.fsm_index())
    j = self.n2.query('SELECT count(*) FROM foo', level='none')
    self.assertEqual(j, d_("{'results': [{'values': [[402]], 'types': [''], 'columns': ['count(*)']}]}"))

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

