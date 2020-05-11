
#default test stuff
import unittest
from zfs_autobackup import *

import subprocess
import time

print("THIS TEST REQUIRES SSH TO LOCALHOST")

class TestExecuteNode(unittest.TestCase):

    def setUp(self):

        return super().setUp()

    def basics(self, node ):

        #single line 
        self.assertEqual(node.run(["echo","test"]), ["test"])

        #error exit code
        with self.assertRaises(subprocess.CalledProcessError):
            node.run(["false"])

        #multiline without tabsplit
        self.assertEqual(node.run(["echo","l1c1\tl1c2\nl2c1\tl2c2"], tab_split=False), ["l1c1\tl1c2", "l2c1\tl2c2"])

        #multiline tabsplit
        self.assertEqual(node.run(["echo","l1c1\tl1c2\nl2c1\tl2c2"], tab_split=True), [['l1c1', 'l1c2'], ['l2c1', 'l2c2']])

        #escaping test (shouldnt be a problem locally, single quotes can be a problem remote via ssh)
        s="><`'\"@&$()$bla\\/.*!#test _+-={}[]|"
        self.assertEqual(node.run(["echo",s]), [s])

        #return std err as well, trigger stderr by listing something non existing
        (stdout, stderr)=node.run(["ls", "nonexistingfile"], return_stderr=True, valid_exitcodes=[2])
        self.assertEqual(stdout,[])
        self.assertRegex(stderr[0],"nonexistingfile")

        #slow command, make sure things dont exit too early
        start_time=time.time()
        node.run(["sleep","1"])
        self.assertGreaterEqual(time.time()-start_time,1)

        #input a string and check it via cat
        self.assertEqual(node.run(["cat"], input="test"), ["test"])


    def test_basicslocal(self):
        node=ExecuteNode(debug_output=True)
        self.basics(node)

    def test_basicsremote(self):
        node=ExecuteNode(ssh_to="localhost", debug_output=True)
        self.basics(node)







if __name__ == '__main__':
    unittest.main()