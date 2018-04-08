#!/usr/bin/env python

from __future__ import print_function
import tempfile
import argparse
import subprocess
import requests
import json
import os
import shutil
import time
import sys
from urlparse import urlparse

DESCRIPTION = '''
Runs a full system test, bringing up a full cluster, issuing commands,
and killing nodes.
'''

class Node(object):
	def __init__(self, path, node_id, api_addr, raft_addr, dir):
		self.path = path
		self.node_id = node_id
		self.api_addr = api_addr
		self.raft_addr = raft_addr
		self.dir = dir
		self.process = None
		self.stdout_file = os.path.join(dir, 'rqlited.log')
		self.stdout_fd = open(self.stdout_file, 'w')
		self.stderr_file = os.path.join(dir, 'rqlited.err')
		self.stderr_fd = open(self.stderr_file, 'w')

	def start(self, join=None, wait=True, timeout=30):
		if self.process is not None:
			return
		command = [self.path,
		           '-node-id', self.node_id,
		           '-http-addr', self.api_addr,
		           '-raft-addr', self.raft_addr]
		if join is not None:
		 	command += ['-join', 'http://' + join]
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

	def status(self):
		r = requests.get(self._status_url())
		r.raise_for_status()
		return r.json()

	def is_leader(self):
		try:
			return self.status()['store']['raft']['state'] == 'Leader'
		except requests.exceptions.ConnectionError:
			return False

	def is_follower(self):
		try:
			return self.status()['store']['raft']['state'] == 'Follower'
		except requests.exceptions.ConnectionError:
			return False

	def wait_for_leader(self, timeout=30):
		l = None
		t = 0
		while l == None or l is '':
			if t > timeout:
				raise Exception('timeout')
			l = self.status()['store']['leader']
			time.sleep(1)
			t+=1
		return l

	def applied_index(self):
		return self.status()['store']['raft']['applied_index']

	def wait_for_applied_index(self, index, timeout=30):
		t = 0
		while self.status()['store']['raft']['applied_index'] != index:
			if t > timeout:
				raise Exception('timeout')
			time.sleep(1)
			t+=1

	def query(self, statement, level='weak'):
		r = requests.get(self._query_url(), params={'q': statement, 'level': level})
		r.raise_for_status()
		return r.json()

	def execute(self, statement):
		r = requests.post(self._execute_url(), data=json.dumps([statement]))
		r.raise_for_status()
		return r.json()

	def redirect_addr(self):
		r = requests.post(self._execute_url(), data=json.dumps(['nonsense']), allow_redirects=False)
		if r.status_code == 301:
			return urlparse(r.headers['Location']).netloc

	def _status_url(self):
		return 'http://' + self.api_addr + '/status'
	def _query_url(self):
		return 'http://' + self.api_addr + '/db/query'
	def _execute_url(self):
		return 'http://' + self.api_addr + '/db/execute'

	def __eq__(self, other):
		return self.node_id == other.node_id
	def __str__(self):
		return '%s:[%s]:[%s]:[%s]' % (self.node_id, self.api_addr, self.raft_addr, self.dir)
	def __del__(self):
		self.stdout_fd.close()
		self.stderr_fd.close()

def deprovision_node(node):
	node.stop()
	shutil.rmtree(node.dir)
	node = None

class Cluster(object):
	def __init__(self, nodes):
		self.nodes = nodes
	def wait_for_leader(self, node_exc=None, timeout=30):
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
	def followers(self):
		return [n for n in self.nodes if n.is_follower()]
	def deprovision(self):
		for n in self.nodes:
			deprovision_node(n)

def log_test(test_name):
	def log_test_decorator(func):
		def func_wrapper(cluster):
			print('%s......' % test_name, end='')
			sys.stdout.flush()
			r = func(cluster)
			print('OK')
			return r
		return func_wrapper
	return log_test_decorator

@log_test('test_election')
def test_election(cluster):
	n = cluster.wait_for_leader().stop()
	cluster.wait_for_leader(node_exc=n)
	n.start()
	n.wait_for_leader()

@log_test('test_execute_fail_rejoin')
def test_execute_fail_rejoin(cluster):
	n = cluster.wait_for_leader()
	j = n.execute('CREATE TABLE test_execute_then_fail (id INTEGER NOT NULL PRIMARY KEY, name TEXT)')
	assert(str(j)=="{u'results': [{}]}")
	j = n.execute('INSERT INTO test_execute_then_fail(name) VALUES("fiona")')
	assert(str(j)=="{u'results': [{u'last_insert_id': 1, u'rows_affected': 1}]}")
	j = n.query('SELECT * FROM test_execute_then_fail')
	assert(str(j)=="{u'results': [{u'values': [[1, u'fiona']], u'types': [u'integer', u'text'], u'columns': [u'id', u'name']}]}")

	n0 = cluster.wait_for_leader().stop()
	n1 = cluster.wait_for_leader(node_exc=n0)
	j = n1.query('SELECT * FROM test_execute_then_fail')
	assert(str(j)=="{u'results': [{u'values': [[1, u'fiona']], u'types': [u'integer', u'text'], u'columns': [u'id', u'name']}]}")
	j = n1.execute('INSERT INTO test_execute_then_fail(name) VALUES("declan")')
	assert(str(j)=="{u'results': [{u'last_insert_id': 2, u'rows_affected': 1}]}")

	n0.start()
	n0.wait_for_leader()
	n0.wait_for_applied_index(n1.applied_index())
	j = n0.query('SELECT * FROM test_execute_then_fail', level='none')
	assert(str(j)=="{u'results': [{u'values': [[1, u'fiona'], [2, u'declan']], u'types': [u'integer', u'text'], u'columns': [u'id', u'name']}]}")

@log_test('test_leader_redirect')
def test_leader_redirect(cluster):
	l = cluster.wait_for_leader()
	fs = cluster.followers()
	assert(len(fs)==2)
	for n in fs:
		assert(l.api_addr==n.redirect_addr())

	l.stop()
	n = cluster.wait_for_leader(node_exc=l)
	f = cluster.followers()[0]
	assert(n.api_addr==f.redirect_addr())
	l.start()
	l.wait_for_leader()

@log_test('setup_cluster')
def setup_cluster(path):
	node0 = Node(path, 'node0', 'localhost:4001', 'localhost:4002', tempfile.mkdtemp())
	node0.start()
	node0.wait_for_leader()

	node1 = Node(path, 'node1', 'localhost:4003', 'localhost:4004', tempfile.mkdtemp())
	node1.start(join='localhost:4001')
	node1.wait_for_leader()

	node2 = Node(path, 'node2', 'localhost:4005', 'localhost:4006', tempfile.mkdtemp())
	node2.start(join='localhost:4001')
	node2.wait_for_leader()

	return Cluster([node0, node1, node2])

def run_test(cluster):
	test_election(cluster)
	test_execute_fail_rejoin(cluster)
	test_leader_redirect(cluster)

	cluster.deprovision()

if __name__ == "__main__":
	parser = argparse.ArgumentParser(prog='full_system_test.py',
		description=DESCRIPTION)
	parser.add_argument('path', help='Path to rqlited binary')
	args = parser.parse_args()

	if not os.path.isfile(args.path):
		raise Exception('binary does not exist')

	run_test(setup_cluster(args.path))
