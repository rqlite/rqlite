#!/usr/bin/env python
#
# To run a specific test, execute
#
#  python system_test/full_system_test.py Class.test


import os
import unittest
import subprocess

from helpers import Node, deprovision_node


RQLITE_PATH = os.environ['RQLITE_PATH']
RQLITED_PATH = os.environ['RQLITED_PATH']

class TestRqliteShell(unittest.TestCase):
    def _shell(self, port, input):
        proc = subprocess.run([RQLITE_PATH, '--port', str(port)],input=input,text=True,capture_output=True,check=False)
        if proc.returncode != 0:
            raise Exception(f"Shell command failed: {proc.stderr}")
        return proc.stdout

    def setUp(self):
        self.node = Node(RQLITED_PATH, '0',  raft_snap_threshold=2, raft_snap_int="1s")
        self.node.start()
        self.node.wait_for_leader()

    def tearDown(self):
        deprovision_node(self.node)
    
    def test(self):
        db_path = "system_test/e2e/testdata/shell.db"

        out = self._shell(self.node.APIPort(), f""".ready""")
        self.assertIn("ready", out)

        out = self._shell(self.node.APIPort(), f""".tables""")
        self.assertNotIn("foo", out)

        out = self._shell(self.node.APIPort(), f""".restore {db_path}""")
        self.assertIn("Database restored successfully.", out)

        out = self._shell(self.node.APIPort(), f""".tables""")
        self.assertIn("foo", out)

        out = self._shell(self.node.APIPort(), f"""SELECT COUNT(*) FROM foo""")
        self.assertIn("1900", out)

        out = self._shell(self.node.APIPort(), f"""INSERT INTO foo(id, name) VALUES (1000000, 'name1000000')""")
        self.assertIn("1 row affected", out)

        out = self._shell(self.node.APIPort(), f"""DROP TABLE foo""")
        out = self._shell(self.node.APIPort(), f"""SELECT COUNT(*) FROM foo""")
        self.assertIn("no such table", out)

if __name__ == "__main__":
  unittest.main(verbosity=2)