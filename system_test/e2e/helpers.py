#!/usr/bin/env python

import tempfile
import ast
import gzip
import subprocess
import requests
import json
import os
import random
import re
from urllib.parse import urlparse
import shutil
import time
import socket
import sqlite3
import string

TIMEOUT=20

seqRe = re.compile("^{'results': \[\], 'sequence_number': \d+}$")

# random_addr returns a random IP:port address which is not already in use,
# and which has not already been returned by this function.
allocated_addresses = set()
def random_addr():
  while True:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
      s.bind(('localhost', 0))
      addr = f"{s.getsockname()[0]}:{s.getsockname()[1]}"
      if addr not in allocated_addresses:
        allocated_addresses.add(addr)
        return addr

def d_(s):
    return ast.literal_eval(s.replace("'", "\""))

def env_present(name):
  return name in os.environ and os.environ[name] != ""

def gunzip_file(path):
  with gzip.open(path, 'rb') as f:
    file_content = f.read()
  return write_random_file(file_content, mode='wb')

def gzip_compress(input_file, output_file):
    with open(input_file, 'rb') as f_in:
        with gzip.open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

def is_sequence_number(r):
  return seqRe.match(r)

def random_string(n):
  letters = string.ascii_lowercase
  return ''.join(random.choice(letters) for i in range(n))

def temp_file():
  f = tempfile.NamedTemporaryFile(delete=False)
  f.close()
  return f.name

def copy_dir_to_temp(src):
  if not os.path.isdir(src):
    raise Exception('src is not a directory')
  temp_dir = tempfile.mkdtemp()
  shutil.copytree(src, temp_dir, dirs_exist_ok=True)
  return temp_dir

def write_random_file(data, mode='w'):
  f = tempfile.NamedTemporaryFile(mode, delete=False)
  f.write(data)
  f.close()
  return f.name

def poll_query(node, query, exp, level='none', timeout=TIMEOUT):
    deadline = time.time() + 3
    while time.time() < deadline:
      j = node.query(query, level=level)
      if j == exp:
        return
      time.sleep(0.1)
    raise Exception('timeout waiting expected query response')

def raise_for_status(r):
  try:
    r.raise_for_status()
  except requests.exceptions.HTTPError as e:
    print(e)
    print((r.text))
    raise e

class Node(object):
  def __init__(self, exe_path, node_id,
               api_addr=None, api_adv=None,
               bootstrap_expect=0,
               raft_addr=None, raft_adv=None,
               raft_voter=True,
               raft_snap_threshold=8192, raft_snap_int="1s",
               raft_cluster_remove_shutdown=False,
               http_cert=None, http_key=None, http_no_verify=False,
               node_cert=None, node_key=None, node_ca_cert=None,
               node_verify_server_name=None, node_no_verify=False,
               auth=None, auto_backup=None, auto_restore=None,
               dir=None):
    
    if api_addr is None:
      api_addr = random_addr()
    if raft_addr is None:
      raft_addr = random_addr()
        
    if api_adv is None:
      api_adv = api_addr
        
    if dir is None:
      dir = tempfile.mkdtemp()
    self.dir = dir
    self.exe_path = exe_path
    self.peers_path = os.path.join(self.dir, "raft/peers.json")
    self.node_id = node_id
    self.api_addr = api_addr
    self.api_adv = api_adv
    self.bootstrap_expect = bootstrap_expect
    self.raft_addr = raft_addr
    self.raft_adv = raft_adv
    self.raft_voter = raft_voter
    self.raft_snap_threshold = raft_snap_threshold
    self.raft_snap_int = raft_snap_int
    self.raft_cluster_remove_shutdown = raft_cluster_remove_shutdown
    self.http_cert = http_cert
    self.http_key = http_key
    self.http_no_verify = http_no_verify
    self.node_cert = node_cert
    self.node_key = node_key
    self.node_no_verify = node_no_verify
    self.node_ca_cert = node_ca_cert
    self.node_verify_server_name = node_verify_server_name
    self.auth = auth
    self.auto_backup = auto_backup
    self.auto_restore = auto_restore
    self.disco_key = random_string(10)
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

  def RaftAddr(self):
      if self.raft_adv is not None:
          return self.raft_adv
      return self.raft_addr

  def scramble_network(self):
    if self.api_adv == self.api_addr:
      self.api_adv = None

    self.api_addr = random_addr()

    if self.api_adv is None:
      self.api_adv = self.api_addr

    if self.raft_adv == self.raft_addr:
      self.raft_adv = None

    self.raft_addr = random_addr()

    if self.raft_adv is None:
      self.raft_adv = self.raft_addr

  def start(self, join=None, join_attempts=None, join_interval="1s", join_as=None,
    disco_mode=None, disco_key=None, disco_config=None, wait=True, timeout=TIMEOUT):
    if self.process is not None:
      return

    command = [self.exe_path,
               '-node-id', self.node_id,
               '-http-addr', self.api_addr,
               '-bootstrap-expect', str(self.bootstrap_expect),
               '-raft-addr', self.raft_addr,
               '-raft-snap', str(self.raft_snap_threshold),
               '-raft-snap-int', self.raft_snap_int,
               '-raft-cluster-remove-shutdown=%s' % str(self.raft_cluster_remove_shutdown).lower(),
               '-raft-non-voter=%s' % str(not self.raft_voter).lower()]
    if self.api_adv is not None:
      command += ['-http-adv-addr', self.api_adv]
    if self.raft_adv is not None:
      command += ['-raft-adv-addr', self.raft_adv]
    if self.http_cert is not None:
      command += ['-http-cert', self.http_cert, '-http-key', self.http_key]
      if self.http_no_verify:
        command += ['-http-no-verify']
    if self.node_cert is not None:
      command += ['-node-cert', self.node_cert, '-node-key', self.node_key]
    if self.node_ca_cert is not None:
      command += ['-node-ca-cert', self.node_ca_cert]
    if self.node_no_verify:
      command += ['-node-no-verify']
    if self.node_verify_server_name is not None:
      command += ['-node-verify-server-name', self.node_verify_server_name]
    if self.auth is not None:
      command += ['-auth', self.auth]
    if self.auto_backup is not None:
      command += ['-auto-backup', self.auto_backup]
    if self.auto_restore is not None:
      command += ['-auto-restore', self.auto_restore]
    if join is not None:
      command += ['-join', join]
    if join_attempts is not None:
       command += ['-join-attempts', str(join_attempts)]
    if join_interval is not None:
       command += ['-join-interval', join_interval]
    if join_as is not None:
        command += ['-join-as', join_as]
    if disco_mode is not None:
      dk = disco_key
      if dk is None:
        dk = self.disco_key
      command += ['-disco-mode', disco_mode, '-disco-key', dk]
      if disco_config is not None:
        command += ['-disco-config', disco_config]
    command.append(self.dir)

    self.process = subprocess.Popen(command, stdout=self.stdout_fd, stderr=self.stderr_fd)
    deadline = time.time() + timeout
    while wait:
      if time.time() > deadline:
        self.dump_log("dumping log due to timeout during start")
        raise Exception('rqlite process failed to start within %d seconds' % timeout)
      try:
        self.status()
      except requests.exceptions.ConnectionError:
        time.sleep(0.1)
      else:
        break
    with open(os.path.join(self.dir, "pid"), "w") as f:
      f.write(str(self.process.pid))
    return self

  def stop(self, graceful=False):
    if self.process is None:
      return
    if graceful:
      self.process.terminate()
    else:
      self.process.kill()
    self.process.wait()
    self.process = None
    return self

  def pid(self):
    if self.process is None:
      return None
    return self.process.pid

  def db_path(self):
    return self.status()['store']['sqlite3']['path']

  def wal_path(self):
    return self.db_path() + "-wal"

  def status(self):
    r = requests.get(self._status_url())
    raise_for_status(r)
    return r.json()

  def pragmas(self):
    r = requests.get(self._status_url())
    raise_for_status(r)
    return r.json()['store']['sqlite3']['pragmas']

  def nodes(self):
    r = requests.get(self._nodes_url())
    raise_for_status(r)
    return r.json()

  def ready(self, noleader=False, sync=False):
    r = requests.get(self._ready_url(noleader, sync))
    return r.status_code == 200

  def expvar(self):
    r = requests.get(self._expvar_url())
    raise_for_status(r)
    return r.json()

  def is_leader(self):
    '''
    is_leader returns whether this node is the cluster leader
    '''
    try:
      return self.status()['store']['raft']['state'] == 'Leader'
    except (KeyError, requests.exceptions.ConnectionError):
      return False

  def is_follower(self):
    try:
      return self.status()['store']['raft']['state'] == 'Follower'
    except requests.exceptions.ConnectionError:
      return False

  def is_voter(self):
    try:
      return self.status()['store']['raft']['voter'] == True
    except requests.exceptions.ConnectionError:
      return False

  def disco_mode(self):
    try:
      return self.status()['disco']['mode']
    except requests.exceptions.ConnectionError:
      return ''

  def wait_for_leader(self, timeout=TIMEOUT, log=True, ready=True):
    lr = None
    deadline = time.time() + timeout
    while lr == None or lr['addr'] == '':
      if time.time() > deadline:
        if log:
          self.dump_log("dumping log due to timeout waiting for leader")
        raise Exception('rqlite node failed to detect leader within %d seconds' % timeout)
      try:
        lr = self.status()['store']['leader']
      except (KeyError, requests.exceptions.ConnectionError):
        pass
      time.sleep(0.1)

    # Perform a check on readyness while we're here.
    if ready and (self.ready() is not True):
      raise Exception('leader is available but node reports not ready')

    # Ensure the leader is sane.
    if lr['node_id'] == '':
      raise Exception('leader is available but node %s at %s reports empty leader node ID' % (self.node_id, self.APIAddr()))
    if lr['addr'] == '':
      raise Exception('leader is available but node %s at %s reports empty leader addr' % (self.node_id, self.APIAddr()))
    return lr

  def wait_for_ready(self, sync=False, timeout=TIMEOUT):
    deadline = time.time() + timeout
    while time.time() < deadline:
      if self.ready(sync):
        return
      time.sleep(0.1)
    raise Exception('rqlite node failed to become ready within %d seconds' % timeout)

  def expect_leader_fail(self, timeout=TIMEOUT):
    try:
      self.wait_for_leader(self, timeout, log=False)
    except:
      return True
    return False

  def db_applied_index(self):
    return int(self.status()['store']['db_applied_index'])

  def fsm_index(self):
    return int(self.status()['store']['fsm_index'])

  def raft_commit_index(self):
    return int(self.status()['store']['raft']['commit_index'])

  def raft_applied_index(self):
    return int(self.status()['store']['raft']['applied_index'])

  def raft_fsm_pending_index(self):
    return int(self.status()['store']['raft']['fsm_pending'])

  def raft_last_log_index(self):
    return int(self.status()['store']['raft']['last_log_index'])

  def raft_last_snapshot_index(self):
    return int(self.status()['store']['raft']['last_snapshot_index'])

  def num_join_requests(self):
    return int(self.expvar()['cluster']['num_join_req'])

  def num_snapshots(self):
    return int(self.expvar()['store']['num_snapshots'])

  def num_restores(self):
    return int(self.expvar()['store']['num_restores'])

  def num_auto_backups(self):
    '''
    Return a dict of the number of successful, failed, skipped auto-backups, skipped sum auto-backups.
    '''
    return {
      'ok': int(self.expvar()['uploader']['num_uploads_ok']),
      'fail': int(self.expvar()['uploader']['num_uploads_fail']),
      'skip': int(self.expvar()['uploader']['num_uploads_skipped']),
      'skip_id': int(self.expvar()['uploader']['num_uploads_skipped_id'])
    }

  def wait_for_upload(self, i, timeout=TIMEOUT):
    '''
    Wait until the number of uploads is equal to the given value.
    '''
    deadline = time.time() + timeout
    while time.time() < deadline:
      if self.num_auto_backups()['ok'] == i:
        return self.num_auto_backups()
      time.sleep(0.1)
    raise Exception('rqlite node failed to upload backup within %d seconds (%s)' % (timeout, self.num_auto_backups()))

  def wait_for_upload_skipped_id(self, i, timeout=TIMEOUT):
    '''
    Wait until the number of skipped ID uploads is at least as great as the given value.
    '''
    deadline = time.time() + timeout
    while time.time() < deadline:
      if self.num_auto_backups()['skip_id'] >= i:
        return self.num_auto_backups()
      time.sleep(0.1)
    raise Exception('rqlite node failed to skip backup due sum within %d seconds (%s)' % (timeout, self.num_auto_backups()))

  def wait_until_uploads_idle(self, timeout=TIMEOUT):
    '''
    Wait until uploads go idle.
    '''
    deadline = time.time() + timeout
    while time.time() < deadline:
      backups = self.num_auto_backups()['ok']
      skipped = self.num_auto_backups()['skip']
      skipped_sum = self.num_auto_backups()['skip_id']
      time.sleep(0.1)
      if self.num_auto_backups()['skip'] + self.num_auto_backups()['skip_id'] == skipped + skipped_sum:
        # Skipped uploads are not increasing, so uploads are not idle
        continue

      # OK, skipped uploads are increasing, but has the number of backups stayed the same?
      if self.num_auto_backups()['ok'] != backups:
        continue

      # Backups are idle
      return self.num_auto_backups()

    n = self.num_auto_backups()
    raise Exception('rqlite node failed to idle backups within %d seconds (%s)' % n)

  def wait_for_fsm_index(self, index, timeout=TIMEOUT):
    '''
    Wait until the given index has been applied to the state machine.
    '''
    deadline = time.time() + timeout
    while self.fsm_index() < index:
      if time.time() > deadline:
        raise Exception('timeout, target index: %d, actual index %d' % (index, self.fsm_index()))
      time.sleep(0.1)
    return self.fsm_index()

  def wait_for_all_applied(self, timeout=TIMEOUT):
    '''
    Wait until the applied index equals the commit index.
    '''
    deadline = time.time() + timeout
    while self.raft_commit_index() != self.raft_applied_index():
      if time.time() > deadline:
        raise Exception('wait_for_all_applied timeout')
      time.sleep(0.1)
    return self.raft_applied_index()

  def wait_for_restores(self, num, timeout=TIMEOUT):
    '''
    Wait until the number of snapshot-restores on this node reach
    the given value.
    '''
    deadline = time.time() + timeout
    while self.num_restores() != num:
      if time.time() > deadline:
        raise Exception('wait_for_restores timeout wanted %d, got %d' % (num, self.num_restores()))
      time.sleep(0.1)
    return self.num_restores()

  def query(self, statement, params=None, level='weak', pretty=False, text=False, associative=False):
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
    if associative:
      reqParams['associative'] = "yes"
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

  def execute_queued(self, statement, wait=False, params=None):
    body = [statement]
    if params is not None:
      try:
        body = body + params
      except TypeError:
        # Presumably not a list, so append as an object.
        body.append(params)
    r = requests.post(self._execute_queued_url(wait), data=json.dumps([body]))
    raise_for_status(r)
    return r.json()

  def request(self, statement, params=None, level='weak', pretty=False, associative=False):
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
    if associative:
      reqParams['associative'] = "yes"
    r = requests.post(self._request_url(), params=reqParams, data=json.dumps([body]))
    raise_for_status(r)
    return r.json()

  def request_raw(self, body):
    r = requests.post(self._request_url(), data=body)
    raise_for_status(r)
    return r.json()

  def backup(self, file):
    with open(file, 'wb') as fd:
      r = requests.get(self._backup_url())
      raise_for_status(r)
      fd.write(r.content)

  def remove_node(self, id):
    body = {"id": id}
    r = requests.delete(self._remove_url(), data=json.dumps(body))
    raise_for_status(r)

  def restore(self, file, fmt=None):
    # This is an API that doesn't expect JSON.
    if fmt != "binary":
      conn = sqlite3.connect(file)
      r = requests.post(self._load_url(), data='\n'.join(conn.iterdump()))
      raise_for_status(r)
      conn.close()
      return r.json()
    else:
      with open(file, 'rb') as f:
        data = f.read()
      r = requests.post(self._load_url(), data=data, headers={'Content-Type': 'application/octet-stream'})
      raise_for_status(r)

  def boot(self, file):
    # This is an API that doesn't expect JSON.
    with open(file, 'rb') as f:
      data = f.read()
      r = requests.post(self._boot_url(), data=data, headers={'Content-Type': 'application/octet-stream'})
      raise_for_status(r)

  def redirect_addr(self):
    r = requests.post(self._execute_url(redirect=True), data=json.dumps(['nonsense']), allow_redirects=False)
    raise_for_status(r)
    if r.status_code == 301:
      return "%s://%s" % (urlparse(r.headers['Location']).scheme, urlparse(r.headers['Location']).netloc)

  def set_peers(self, peers):
    f = open(self.peers_path, "w")
    f.write(json.dumps(peers))
    f.close()

  def dump_log(self, msg):
    print(msg)
    self.stderr_fd.close()
    f = open(self.stderr_file, 'r')
    for l in f.readlines():
          print(l.strip())

  def _status_url(self):
    return 'http://' + self.APIAddr() + '/status'
  def _nodes_url(self):
    return 'http://' + self.APIAddr() + '/nodes?nonvoters' # Getting all nodes back makes testing easier
  def _ready_url(self, noleader=False, sync=False):
    vals = []
    nl = ""
    if noleader:
      vals = vals + ["noleader"]
    if sync:
      vals = vals + ["sync"]
    nl = '?' + '&'.join(vals)
    return 'http://' + self.APIAddr() + '/readyz' + nl
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
  def _execute_queued_url(self, wait=False):
    u = '/db/execute?queue'
    if wait:
      u = u + '&wait'
    return 'http://' + self.APIAddr() + u
  def _request_url(self, redirect=False):
    rd = ""
    if redirect:
      rd = "?redirect"
    return 'http://' + self.APIAddr() + '/db/request' + rd
  def _backup_url(self):
    return 'http://' + self.APIAddr() + '/db/backup'
  def _load_url(self):
    return 'http://' + self.APIAddr() + '/db/load'
  def _boot_url(self):
    return 'http://' + self.APIAddr() + '/boot'
  def _remove_url(self):
    return 'http://' + self.APIAddr() + '/remove'
  def __eq__(self, other):
    return self.node_id == other.node_id
  def __str__(self):
    return '%s:[%s]:[%s]:[%s]' % (self.node_id, self.APIAddr(), self.raft_addr, self.dir)
  def __del__(self):
    self.stdout_fd.close()
    self.stderr_fd.close()

def deprovision_node(node):
  node.stop()
  if os.path.isdir(node.dir):
    shutil.rmtree(node.dir)
  node = None

class Cluster(object):
  def __init__(self, nodes):
    self.nodes = nodes
  def wait_for_leader(self, node_exc=None, timeout=TIMEOUT):
    deadline = time.time() + timeout
    while True:
      if time.time() > deadline:
        raise Exception('timeout')
      for n in self.nodes:
        if node_exc is not None and n == node_exc:
          continue
        if n.is_leader():
          return n
      time.sleep(0.1)
  def cross_check_leader(self):
    '''
    Check that all nodes agree on who the leader is.
    '''
    leaders = []
    for n in self.nodes:
      leaders.append(n.wait_for_leader()['node_id'])
    return set(leaders) == 1
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
