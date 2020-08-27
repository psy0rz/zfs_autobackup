from basetest import *


print("THIS TEST REQUIRES SSH TO LOCALHOST")

class TestExecuteNode(unittest2.TestCase):

    # def setUp(self):

    #     return super().setUp()

    def basics(self, node ):

        with self.subTest("simple echo"):
            self.assertEqual(node.run(["echo","test"]), ["test"])

        with self.subTest("error exit code"):
            with self.assertRaises(subprocess.CalledProcessError):
                node.run(["false"])

        #
        with self.subTest("multiline without tabsplit"):
            self.assertEqual(node.run(["echo","l1c1\tl1c2\nl2c1\tl2c2"], tab_split=False), ["l1c1\tl1c2", "l2c1\tl2c2"])

        #multiline tabsplit
        with self.subTest("multiline tabsplit"):
            self.assertEqual(node.run(["echo","l1c1\tl1c2\nl2c1\tl2c2"], tab_split=True), [['l1c1', 'l1c2'], ['l2c1', 'l2c2']])

        #escaping test (shouldnt be a problem locally, single quotes can be a problem remote via ssh)
        with self.subTest("escape test"):
            s="><`'\"@&$()$bla\\/.*!#test _+-={}[]|"
            self.assertEqual(node.run(["echo",s]), [s])

        #return std err as well, trigger stderr by listing something non existing
        with self.subTest("stderr return"):
            (stdout, stderr)=node.run(["ls", "nonexistingfile"], return_stderr=True, valid_exitcodes=[2])
            self.assertEqual(stdout,[])
            self.assertRegex(stderr[0],"nonexistingfile")

        #slow command, make sure things dont exit too early
        with self.subTest("early exit test"):
            start_time=time.time()
            self.assertEqual(node.run(["sleep","1"]), [])
            self.assertGreaterEqual(time.time()-start_time,1)

        #input a string and check it via cat
        with self.subTest("stdin input string"):
            self.assertEqual(node.run(["cat"], inp="test"), ["test"])

        #command that wants input, while we dont have input, shouldnt hang forever.
        with self.subTest("stdin process with inp=None (shouldn't hang)"):
            self.assertEqual(node.run(["cat"]), [])

    def test_basics_local(self):
        node=ExecuteNode(debug_output=True)
        self.basics(node)

    def test_basics_remote(self):
        node=ExecuteNode(ssh_to="localhost", debug_output=True)
        self.basics(node)

    ################

    def test_readonly(self):
        node=ExecuteNode(debug_output=True, readonly=True)

        self.assertEqual(node.run(["echo","test"], readonly=False), None)
        self.assertEqual(node.run(["echo","test"], readonly=True), ["test"])


    ################

    def pipe(self, nodea, nodeb):

        with self.subTest("pipe data"):
            output=nodea.run(["dd", "if=/dev/zero", "count=1000"], pipe=True)
            self.assertEqual(nodeb.run(["md5sum"], inp=output), ["816df6f64deba63b029ca19d880ee10a  -"])

        with self.subTest("exit code both ends of pipe ok"):
            output=nodea.run(["true"], pipe=True)
            nodeb.run(["true"], inp=output)
    
        with self.subTest("error on pipe input side"):
            with self.assertRaises(subprocess.CalledProcessError):
                output=nodea.run(["false"], pipe=True)
                nodeb.run(["true"], inp=output)

        with self.subTest("error on pipe output side "):
            with self.assertRaises(subprocess.CalledProcessError):
                output=nodea.run(["true"], pipe=True)
                nodeb.run(["false"], inp=output)

        with self.subTest("error on both sides of pipe"):
            with self.assertRaises(subprocess.CalledProcessError):
                output=nodea.run(["false"], pipe=True)
                nodeb.run(["false"], inp=output)

        with self.subTest("check stderr on pipe output side"):
            output=nodea.run(["true"], pipe=True)
            (stdout, stderr)=nodeb.run(["ls", "nonexistingfile"], inp=output, return_stderr=True, valid_exitcodes=[0,2])
            self.assertEqual(stdout,[])
            self.assertRegex(stderr[0], "nonexistingfile" )

        with self.subTest("check stderr on pipe input side (should be only printed)"):
            output=nodea.run(["ls", "nonexistingfile"], pipe=True)
            (stdout, stderr)=nodeb.run(["true"], inp=output, return_stderr=True, valid_exitcodes=[0,2])
            self.assertEqual(stdout,[])
            self.assertEqual(stderr,[] )
            



    def test_pipe_local_local(self):
        nodea=ExecuteNode(debug_output=True)
        nodeb=ExecuteNode(debug_output=True)
        self.pipe(nodea, nodeb)

    def test_pipe_remote_remote(self):
        nodea=ExecuteNode(ssh_to="localhost", debug_output=True)
        nodeb=ExecuteNode(ssh_to="localhost", debug_output=True)
        self.pipe(nodea, nodeb)

    def test_pipe_local_remote(self):
        nodea=ExecuteNode(debug_output=True)
        nodeb=ExecuteNode(ssh_to="localhost", debug_output=True)
        self.pipe(nodea, nodeb)

    def test_pipe_remote_local(self):
        nodea=ExecuteNode(ssh_to="localhost", debug_output=True)
        nodeb=ExecuteNode(debug_output=True)
        self.pipe(nodea, nodeb)


if __name__ == '__main__':
    unittest.main()