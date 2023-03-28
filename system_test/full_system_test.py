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
import re
import shutil
import time
import socket
import sqlite3
import string
import sys
import unittest

from urllib.parse import urlparse

RQLITED_PATH = os.environ['RQLITED_PATH']
TIMEOUT=20

x509cert = '''-----BEGIN CERTIFICATE-----
MIIFXTCCA0WgAwIBAgIJALrA6P0W35jRMA0GCSqGSIb3DQEBCwUAMEUxCzAJBgNV
BAYTAlVTMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQwHhcNMTcwNjEwMjIwMDM1WhcNMTgwNjEwMjIwMDM1WjBF
MQswCQYDVQQGEwJVUzETMBEGA1UECAwKU29tZS1TdGF0ZTEhMB8GA1UECgwYSW50
ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIIC
CgKCAgEA2cxg1IcP1gDQezLJm9MDkEEHqOZEAn1iatoIHUoIlfu36Sripn4yoTxM
1pmOT37CFoaiRfj0biEbjrgfi0QXk9z4E7Vy0XGF6XB5KofOneqnUuSgnOnEkL0p
gQ3itCr/FLkvuT8/zYKL+PXsMnfHGORgJmHlu1/4rY6Z/dayaf4fUFlKRRziEVUn
3EMd/hHFHThXimWd3mtxE1YnpKimnFLmIYjXrK22QUZJ2MYVcRklJYaXhIJgHW2s
oe+ZRhFHxcYoY3znRFZXYkoCXETcExCmo7czLoN4/F92zFDEGbAMbwC/7Zo9AxQg
30Q4iCrLfwAx+M/0A2dRbSTqGReBeBVfEBWopfz7zV3W7kI+s5K2AIFi+1hbmJ6a
mKomv3f4z6Ml+yOqrq4KtrDSxnSf6Vh7EHsws6uyMG7Y6rLpPm1sLDiffPABlAti
/YlVT+3vlg86h7Vlw68CcNSclgyfFW+i1e5a+EV7WB0VmIQXzSkhA86b9aD8qWdL
N4H8sRlSZ3XfIil4u93QDC/NzJl22wRsN7926xR4DgbCesEsc361KYE8fBSx61fa
6EyvlQoI2I4r1aWCSHq7YGfV6guBZekR0BeaIsoNwfZDZrboL0sOrHGxiEfzYdVC
pAxjdG13zuPo+634fUfewBAq695kVYcy3aBt2wOkLyQGLu0CHHsCAwEAAaNQME4w
HQYDVR0OBBYEFAYLLJUqmUdXCNYTQIWX1ICBKGvWMB8GA1UdIwQYMBaAFAYLLJUq
mUdXCNYTQIWX1ICBKGvWMAwGA1UdEwQFMAMBAf8wDQYJKoZIhvcNAQELBQADggIB
AGnvTPevCooo3xO8U/lq2YNo3cFxYnkurBwAn5pwzJrtPgogwezBltEp52+n39TY
5hSa//pfKdQz2GrQ9YvX1vB8gWkNLxBe6g2ksn0DsGTApC/te1p4M+yTKhogtE7a
qYmZBSEI46URe0JLYNirzdTu5dri7DzxFc7E/XlQ0riuMyHNqOP0JXKhxKN1dYOu
NEPxekq2Z2phoo1ul8hBXsz4IRwVeQOAtpRnfrKjxogOI1teP/RSikTsSLvFHxqo
UHVzwBexQs9isBlBUcmuKksxoGugqqSkGQRE+dSs5RSeEPLexMgACfFmKfpS+Vn4
ikb2ETQ3i76+JgMoDHKwb4u9xIyKTUToIsx5dUO+o7paPfyqRE6WbO4H+suM4VCd
VhNbG9qv02Fl8vdYAc/A6tVyV8b4fMbSsGEQnBlvKuOXf/uxAIcz11WUQ4gy/0/e
kHbMqGuBFPkg5nww3dBxkrBbtKq/1yrnQUjpBvjYtyUvoKrLSbQSGj586i52r4hF
+bqGPTxmk6hU4JZN+0wvkbVWLZBTRVNKs8Sb6fRWTd2Zd/o7a7QFhbnnAhv8bgyb
4472yLaXTL/siml+LlSrNGeZEsAaCVH4ETp+HzjpAMAyhhFGqCixG0e9BRPGV936
H/8+SUQK5KxnwDz3hqrAVJyimrvNlSaP1eZ5P8WXuvBl
-----END CERTIFICATE-----'''

x509key = '''-----BEGIN PRIVATE KEY-----
MIIJQQIBADANBgkqhkiG9w0BAQEFAASCCSswggknAgEAAoICAQDZzGDUhw/WANB7
Msmb0wOQQQeo5kQCfWJq2ggdSgiV+7fpKuKmfjKhPEzWmY5PfsIWhqJF+PRuIRuO
uB+LRBeT3PgTtXLRcYXpcHkqh86d6qdS5KCc6cSQvSmBDeK0Kv8UuS+5Pz/Ngov4
9ewyd8cY5GAmYeW7X/itjpn91rJp/h9QWUpFHOIRVSfcQx3+EcUdOFeKZZ3ea3ET
ViekqKacUuYhiNesrbZBRknYxhVxGSUlhpeEgmAdbayh75lGEUfFxihjfOdEVldi
SgJcRNwTEKajtzMug3j8X3bMUMQZsAxvAL/tmj0DFCDfRDiIKst/ADH4z/QDZ1Ft
JOoZF4F4FV8QFail/PvNXdbuQj6zkrYAgWL7WFuYnpqYqia/d/jPoyX7I6qurgq2
sNLGdJ/pWHsQezCzq7Iwbtjqsuk+bWwsOJ988AGUC2L9iVVP7e+WDzqHtWXDrwJw
1JyWDJ8Vb6LV7lr4RXtYHRWYhBfNKSEDzpv1oPypZ0s3gfyxGVJndd8iKXi73dAM
L83MmXbbBGw3v3brFHgOBsJ6wSxzfrUpgTx8FLHrV9roTK+VCgjYjivVpYJIertg
Z9XqC4Fl6RHQF5oiyg3B9kNmtugvSw6scbGIR/Nh1UKkDGN0bXfO4+j7rfh9R97A
ECrr3mRVhzLdoG3bA6QvJAYu7QIcewIDAQABAoICAEKMgXXPAxa3zvwl65ZyZp9Y
T3fbTCKan0zY7CvO6EqzzGExmmmXG+9KVowoBWTi7XkmkETjKgTQlvQH7JOILdAf
b6nOApRepLVMialmL8ru3Uul0jG/+DDlq93kGUZF8QUrBJsM6XjpD831jsNo9+vy
NDLmLOURERIvBXybco6SeIz7i4cMqUL0iyZxV6O/WERyZ8VBAXjpyXZIF/rnEWmo
purOPmBj/9F4Ia5b8EdLkJ8jvf5eO/IiBeLBLEtNkmmq/8JOcvfdjfvZc1kwLTKi
HtjdbIUk5P3wSYNqllDnCxWL3BlEzKm5J8YwuTlaIi3fKGXHXN8BXc8EvYcHOKah
K89HIuexjQyQ0JAWKIIJTZs8jVvTMTjgYnEAB+sLfehBBOKmRdmYij28kIo18blx
tsx1HjdfImDd0QloofRW1Srp6FhcgDK0qfWXze/Vm6IfF40oTVE3fS/RgYzx0SSM
2pc6hTXOnrw1r/UBPyNkJ1D/4UK4m0x91BvTSi6MsThWnhicoaTZl1GP4Qpeo9+4
9Z7t0Yalm0PA55aiHZsm9S8OroasVun2QnDxfUC44PIov7nhqifGVcKA8hIDgSNT
WP8amq9cNjft5xQnP/y70fbioPPiwau2+Q0SXVn/BYxjqZrNp6OfbWSi2IRO1NOD
QZDo2rtnL1RrDdBmtDShAoIBAQD2MTJT6HNacu8+u7DDtbMdKvy7rXVblGbUouh9
cLWX7/zGVcNzB5GSVki3J7J+Kdrs4H45/1JR1YvWtWd93F/xKXmCGkUQCsturtRn
IX93by3zuWNdLv4giP2pk97wNYaaJWZmo87nXKV6BbL//eEl+Ospg5lGrLCsj+Mk
9V8oBBxsxqgVZYVyoevLDAuwUw3Cb52PhnEaLrv30ljGFHpsYb9lFlMs8vRosVWy
i3/T5ASfdnMXKQ1gxN/aPtN6yrFVpXe+S/A5JBzAQfrjiZk4SzNvE7R0eze1YLfO
IulTvlqpk3HVQEpgfq8D3l1x/zqsh0SpCH3VkV5sQQx262iRAoIBAQDieZsWv9eO
QzF5nZmh3NL53LkpqONRBjF5b9phppgxw2jiS8D2eEn2XWExmEaK/JppmzvfxSG4
cPaQHJFjkRGpnJyBlBUnyk4ua5hlXOTb9l5HsLIBlVdcWxwF+zJh8Ylwau+mcVF8
b8n86zke88du+xTvXfMDn6p6EACmBncyZGi424hSw72u8jS0cdmqJl3isLR6duG3
4yipWhEpLU5YuR+796jmjK5h+HQwl/Ra2dykbAw7vN0ofdK0+7LrVnGh7dDecOGK
0fElgFPTazeQQV+dEzqz6UwO0Z9koxqBwPqCLi7sXOeUWwqqb3ewYO7TMM4NlK/o
C8oG4yvWj9pLAoIBACEI9PHhbSkj5wqJ8OwyA3jUfdlJK0hAn5PE0GGUsClVIJwU
ggd7aoMyZMt+3iqjvyat8QIjSo6EkyEacmqnGZCoug9FKyM975JIj2PPUOVb29Sq
ebTVS3BeMXuBxhaBeDBS+GypamgNPH8lKKHFFWMdBaEqcXTUU1i0bgxViJE8C/xk
o8VLPB7nr1YtpZvhaSVACOprZd3Xi41zgkoCEXNdomsUFdEgQL+TnCY7Jcnu/NfQ
8xyWe58Si98jMwl1DVqqu2ijk/Z27Ay4TcweeJrfLGWpRTukFROXiNJ2SMzd7Bh5
Gns9Bz3vgdiJDAzx7JOeCw6LfycbPIpWKDAE4qECggEAZ5kPG7H4Dcio6iPwsj1M
eSXBwc/S5C58FTvYXtERT7o+0T2r8FMIKl1+52vr4Qo6LFLpaaxIh5GNCFE5JJ2o
wbi1UwUFRGVjrBJl7QA4ZHJnoE2wr8673rCCui21V15g637PT4kIqG6OrFaBk6oa
MadDZVfJoX+5QQru8QOGJRQPX3h0/L8zlsKO33gxBId2bQs+E8Mr761G3Wko7nge
HbHZVWet6IC0CHbZ15y7F5APQVt3oR/83tfnughlSQgLBPK/l/F1CsaMlAYG0nB6
Q0/USAsS0FfJBgJX8nY12uMG9OPhbRf2i0O2Nk61JobA2PS7XTUF3pT9/naOiCDX
zwKCAQBK9dPzoc9CfdAJVWjCqR2xS8Jr3/64k59Pya6e7Y4ca+e1g4N7hhQRLPW7
owTKloXrR0mAkwOIiJlk+gXsl2banuSajiPxumSfPYWE1Q/QNFD/WoPvo6rPYJ8N
yA/ORsMjWq51SfpzOU69+FdY7p3GvIVWhRtinqseaAIMOkNZBLVDXF4DvtFgiLZM
bKAjGuXsKOT3MPFU9tHxi4q/7flUb30mSUVXyPjh+C+UH7e0BS0pi/rDeRdEju4z
bJVERP8/VAJ61TDQJq+Il95fzKe4yTA3dDHnO+EG5W2eCsawTK4Ze5XAWqomgdew
62D3AkJQiflLfJL8zTFph1FZXLOm
-----END PRIVATE KEY-----'''

seqRe = re.compile("^{'results': \[\], 'sequence_number': \d+}$")

def d_(s):
    return ast.literal_eval(s.replace("'", "\""))

def is_sequence_number(r):
  return seqRe.match(r)

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
               boostrap_expect=0,
               raft_addr=None, raft_adv=None,
               raft_voter=True,
               raft_snap_threshold=8192, raft_snap_int="1s",
               http_cert=None, http_key=None, http_no_verify=False,
               node_cert=None, node_key=None, node_no_verify=False,
               auth=None, dir=None, on_disk=False):
    
    s_api = None
    s_raft = None
    if api_addr is None:
      s_api, addr = random_addr()
      api_addr = addr
    if raft_addr is None:
      s_raft, addr = random_addr()
      raft_addr = addr
    
    # Only now close any sockets used to get random addresses, so there is
    # no chance a randomly selected address would get re-used by the HTTP
    # system and Raft system.
    if s_api is not None:
        s_api.close()
    if s_raft is not None:
        s_raft.close()
        
    if api_adv is None:
      api_adv = api_addr
        
    if dir is None:
      dir = tempfile.mkdtemp()
    self.dir = dir
    self.path = path
    self.peers_path = os.path.join(self.dir, "raft/peers.json")
    self.node_id = node_id
    self.api_addr = api_addr
    self.api_adv = api_adv
    self.boostrap_expect = boostrap_expect
    self.raft_addr = raft_addr
    self.raft_adv = raft_adv
    self.raft_voter = raft_voter
    self.raft_snap_threshold = raft_snap_threshold
    self.raft_snap_int = raft_snap_int
    self.http_cert = http_cert
    self.http_key = http_key
    self.http_no_verify = http_no_verify
    self.node_cert = node_cert
    self.node_key = node_key
    self.node_no_verify = node_no_verify
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
               '-bootstrap-expect', str(self.boostrap_expect),
               '-raft-addr', self.raft_addr,
               '-raft-snap', str(self.raft_snap_threshold),
               '-raft-snap-int', self.raft_snap_int,
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
      command += ['-node-encrypt', '-node-cert', self.node_cert, '-node-key', self.node_key]
      if self.node_no_verify:
        command += ['-node-no-verify']
    if self.on_disk:
      command += ['-on-disk']
    if self.auth is not None:
      command += ['-auth', self.auth]
    if join is not None:
      if join.startswith('http://') is False:
        join = 'http://' + join
      command += ['-join', join]
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
        self.dump_log("dumping log due to timeout during start")
        raise Exception('rqlite process failed to start within %d seconds' % timeout)
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

  def ready(self, noleader=False):
    r = requests.get(self._ready_url(noleader))
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
    except requests.exceptions.ConnectionError:
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

  def wait_for_leader(self, timeout=TIMEOUT, log=True):
    lr = None
    t = 0
    while lr == None or lr['addr'] == '':
      if t > timeout:
        if log:
          self.dump_log("dumping log due to timeout waiting for leader")
        raise Exception('rqlite node failed to detect leader within %d seconds' % timeout)
      try:
        lr = self.status()['store']['leader']
      except (KeyError, requests.exceptions.ConnectionError):
        pass
      time.sleep(1)
      t+=1

    # Perform a check on readyness while we're here.
    if self.ready() is not True:
      raise Exception('leader is available but node reports not ready')
    return lr

  def expect_leader_fail(self, timeout=TIMEOUT):
    try:
      self.wait_for_leader(self, timeout, log=False)
    except:
      return True
    return false

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

  def num_snapshots(self):
    return int(self.expvar()['store']['num_snapshots'])

  def num_restores(self):
    return int(self.expvar()['store']['num_restores'])

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
        raise Exception('wait_for_commit_index timeout')
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
        raise Exception('wait_for_all_applied timeout')
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
        raise Exception('wait_for_all_fsm timeout')
      time.sleep(1)
      t+=1
    return self.fsm_index()

  def wait_for_restores(self, num, timeout=TIMEOUT):
    '''
    Wait until the number of snapshot-restores on this node reach
    the given value.
    '''
    t = 0
    while self.num_restores() != num:
      if t > timeout:
        raise Exception('wait_for_restores timeout wanted %d, got %d' % (num, self.num_restores()))
      time.sleep(1)
      t+=1
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
    # This is the one API that doesn't expect JSON.
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
  def _ready_url(self, noleader=False):
    nl = ""
    if noleader:
      nl = "?noleader"
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
  def _backup_url(self):
    return 'http://' + self.APIAddr() + '/db/backup'
  def _load_url(self):
    return 'http://' + self.APIAddr() + '/db/load'
  def _remove_url(self):
    return 'http://' + self.APIAddr() + '/remove'
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

    j = n.execute('INSERT INTO bar(name) VALUES("declan")')
    applied = n.wait_for_all_fsm()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 2, 'rows_affected': 1}]}"))
    j = n.query('SELECT * from bar where id=2')
    self.assertEqual(j, d_("{'results': [{'values': [[2, 'declan']], 'types': ['integer', 'text'], 'columns': ['id', 'name']}]}"))

    # Ensure raw response from API is as expected.
    j = n.query('SELECT * from bar', text=True)
    self.assertEqual(str(j), '{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"fiona"],[2,"declan"]]}]}')

    # Ensure raw associative response from API is as expected.
    j = n.query('SELECT * from bar', text=True, associative=True)
    self.assertEqual(str(j), '{"results":[{"types":{"id":"integer","name":"text"},"rows":[{"id":1,"name":"fiona"},{"id":2,"name":"declan"}]}]}')

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

    j = n.execute('INSERT INTO bar(name, age) VALUES(?,?)', params=["declan", None])
    applied = n.wait_for_all_fsm()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 2, 'rows_affected': 1}]}"))

    j = n.query('SELECT * from bar WHERE age=?', params=[20])
    self.assertEqual(j, d_("{'results': [{'values': [[1, 'fiona', 20]], 'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))
    j = n.query('SELECT * from bar WHERE age=?', params=[21])
    self.assertEqual(j, d_("{'results': [{'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))
    j = n.query('SELECT * from bar WHERE name=?', params=['declan'])
    self.assertEqual(j, d_("{'results': [{'values': [[2, 'declan', None]], 'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))

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

    j = n.execute('INSERT INTO bar(name, age) VALUES(?,?)', params=["declan", None])
    applied = n.wait_for_all_fsm()
    self.assertEqual(j, d_("{'results': [{'last_insert_id': 2, 'rows_affected': 1}]}"))

    j = n.query('SELECT * from bar WHERE name=:name', params={"name": "declan"})
    self.assertEqual(j, d_("{'results': [{'values': [[2, 'declan', None]], 'types': ['integer', 'text', 'integer'], 'columns': ['id', 'name', 'age']}]}"))

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
      nSnaps = n.num_snapshots()
      if nSnaps > 0:
        return
      if t > timeout:
        raise Exception('timeout', nSnaps)
      time.sleep(1)
      t+=1

class TestSingleNodeOnDisk(TestSingleNode):
  def setUp(self):
    n0 = Node(RQLITED_PATH, '0',  raft_snap_threshold=2, raft_snap_int="1s", on_disk=True)
    n0.start()
    n0.wait_for_leader()
    self.cluster = Cluster([n0])

class TestSingleNodeReadyz(unittest.TestCase):
  def test(self):
    ''' Test /readyz behaves correctly'''
    n0 = Node(RQLITED_PATH, '0')
    n0.start(join="http://nonsense")
    self.assertEqual(False, n0.ready())
    self.assertEqual(True, n0.ready(noleader=True))
    self.assertEqual(False, n0.ready(noleader=False))
    deprovision_node(n0)

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

class TestSingleNodeEncryptedNoVerify(unittest.TestCase):
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

