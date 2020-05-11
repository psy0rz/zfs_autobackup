
#default test stuff
import unittest
from zfs_autobackup import *

import subprocess

print("THIS TEST REQUIRES SSH TO LOCALHOST")

class TestExecuteNode(unittest.TestCase):

    def setUp(self):

        return super().setUp()

    def basics(self, node ):

        #single line with spaces
        self.assertEqual(node.run(["echo","test test"]), ["test test"])

        #error exit code
        with self.assertRaises(subprocess.CalledProcessError):
            node.run(["false"])

        #multiline without tabsplit
        self.assertEqual(node.run(["echo","l1c1\tl1c2\nl2c1\tl2c2"], tab_split=False), ["l1c1\tl1c2", "l2c1\tl2c2"])
        # self.assertEqual(node.run(["echo","l1\nl2"]), ["l1", "l2"])

        #multiline tabsplit
        self.assertEqual(node.run(["echo","l1c1\tl1c2\nl2c1\tl2c2"], tab_split=True), [['l1c1', 'l1c2'], ['l2c1', 'l2c2']])


        #escaping (shouldnt be a problem locally, single quotes can be a problem remote via ssh)
        s="><`'\"@&$()$bla\\//.*!#test"
        self.assertEqual(node.run(["echo",s]), [s])

        #return std err
        # self.assertEqual(node.run(["echo","test test"], return_stderr=True), ["test test"])


    def test_basicslocal(self):
        node=ExecuteNode(debug_output=True)
        self.basics(node)

    def test_basicsremote(self):
        node=ExecuteNode(ssh_to="localhost", debug_output=True)
        self.basics(node)


    # def test_remoteecho(self):
    #     node=ExecuteNode(ssh_to="localhost", debug_output=True)
    #     self.assertEqual(node.run(["echo","test"]), ["test"])

    # def test_exitlocal(self):
    #     node=ExecuteNode(debug_output=True)

    # def test_exitremote(self):
    #     node=ExecuteNode(ssh_to="localhost", debug_output=True)
    #     with self.assertRaises(subprocess.CalledProcessError):
    #         self.remote1.run(["false"])





if __name__ == '__main__':
    unittest.main()