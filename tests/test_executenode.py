from basetest import *
from zfs_autobackup.ExecuteNode import *

print("THIS TEST REQUIRES SSH TO LOCALHOST")

class TestExecuteNode(unittest2.TestCase):

    # def setUp(self):

    #     return super().setUp()

    def basics(self, node ):

        with self.subTest("simple echo"):
            self.assertEqual(node.run(["echo","test"]), ["test"])

        with self.subTest("error exit code"):
            with self.assertRaises(ExecuteError):
                node.run(["false"])

        #
        with self.subTest("multiline without tabsplit"):
            self.assertEqual(node.run(["echo","l1c1\tl1c2\nl2c1\tl2c2"], tab_split=False), ["l1c1\tl1c2", "l2c1\tl2c2"])

        #multiline tabsplit
        with self.subTest("multiline tabsplit"):
            self.assertEqual(node.run(["echo","l1c1\tl1c2\nl2c1\tl2c2"], tab_split=True), [['l1c1', 'l1c2'], ['l2c1', 'l2c2']])

        #escaping test
        with self.subTest("escape test"):
            s="><`'\"@&$()$bla\\/.* !#test _+-={}[]|${bla} $bla"
            self.assertEqual(node.run(["echo",s]), [s])

        #return std err as well, trigger stderr by listing something non existing
        with self.subTest("stderr return"):
            (stdout, stderr)=node.run(["sh", "-c", "echo bla >&2"], return_stderr=True, valid_exitcodes=[0])
            self.assertEqual(stdout,[])
            self.assertRegex(stderr[0],"bla")

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

        # let the system do the piping with an unescaped |:
        with self.subTest("system piping test"):

            #first make sure the actual | character is still properly escaped:
            self.assertEqual(node.run(["echo","|"]), ["|"])

            #now pipe
            self.assertEqual(node.run(["echo", "abc", node.PIPE, "tr", "a", "A" ]), ["Abc"])

    def test_basics_local(self):
        node=ExecuteNode(debug_output=True)
        self.basics(node)

    def test_basics_remote(self):
        node=ExecuteNode(ssh_to="localhost", debug_output=True)
        self.basics(node)

    ################

    def test_readonly(self):
        node=ExecuteNode(debug_output=True, readonly=True)

        self.assertEqual(node.run(["echo","test"], readonly=False), [])
        self.assertEqual(node.run(["echo","test"], readonly=True), ["test"])


    ################

    def pipe(self, nodea, nodeb):

        with self.subTest("pipe data"):
            output=nodea.run(["dd", "if=/dev/zero", "count=1000"],pipe=True)
            self.assertEqual(nodeb.run(["md5sum"], inp=output), ["816df6f64deba63b029ca19d880ee10a  -"])

        with self.subTest("exit code both ends of pipe ok"):
            output=nodea.run(["true"], pipe=True)
            nodeb.run(["true"], inp=output)

        with self.subTest("error on pipe input side"):
            with self.assertRaises(ExecuteError):
                output=nodea.run(["false"], pipe=True)
                nodeb.run(["true"], inp=output)

        with self.subTest("error on both sides, ignore exit codes"):
            output=nodea.run(["false"], pipe=True, valid_exitcodes=[])
            nodeb.run(["false"], inp=output, valid_exitcodes=[])

        with self.subTest("error on pipe output side "):
            with self.assertRaises(ExecuteError):
                output=nodea.run(["true"], pipe=True)
                nodeb.run(["false"], inp=output)

        with self.subTest("error on both sides of pipe"):
            with self.assertRaises(ExecuteError):
                output=nodea.run(["false"], pipe=True)
                nodeb.run(["false"], inp=output)

        with self.subTest("check stderr on pipe output side"):
            output=nodea.run(["true"], pipe=True, valid_exitcodes=[0])
            (stdout, stderr)=nodeb.run(["sh", "-c", "echo bla >&2"], inp=output, return_stderr=True, valid_exitcodes=[0])
            self.assertEqual(stdout,[])
            self.assertRegex(stderr[0], "bla" )

        with self.subTest("check stderr on pipe input side (should be only printed)"):
            output=nodea.run(["sh", "-c", "echo bla >&2"], pipe=True, valid_exitcodes=[0])
            (stdout, stderr)=nodeb.run(["true"], inp=output, return_stderr=True, valid_exitcodes=[0])
            self.assertEqual(stdout,[])
            self.assertEqual(stderr,[])


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


    def test_cwd(self):

        nodea=ExecuteNode(ssh_to="localhost", debug_output=True)
        nodeb=ExecuteNode(debug_output=True)

        #change to a directory with a space and execute a system pipe, check if all piped commands are executed in correct directory.
        shelltest("mkdir '/tmp/space test' 2>/dev/null; true")
        self.assertEqual(nodea.run(cmd=["pwd", ExecuteNode.PIPE, "cat"], cwd="/tmp/space test"), ["/tmp/space test"])
        self.assertEqual(nodea.run(cmd=["cat", ExecuteNode.PIPE, "pwd"], cwd="/tmp/space test"), ["/tmp/space test"])
        self.assertEqual(nodeb.run(cmd=["pwd", ExecuteNode.PIPE, "cat"], cwd="/tmp/space test"), ["/tmp/space test"])
        self.assertEqual(nodeb.run(cmd=["cat", ExecuteNode.PIPE, "pwd"], cwd="/tmp/space test"), ["/tmp/space test"])

    def test_script_handlers(self):

        def test(node):
            results = []
            node.script(lines=["echo line1", "echo line2 1>&2", "exit 123"],
                                  stdout_handler=lambda line: results.append(line),
                                  stderr_handler=lambda line: results.append(line),
                                  exit_handler=lambda exit_code: results.append(exit_code),
                                  valid_exitcodes=[123]
                                  )

            self.assertEqual(results, ["line1", "line2", 123 ])

        with self.subTest("remote"):
            test(ExecuteNode(ssh_to="localhost", debug_output=True))
        #
        with self.subTest("local"):
            test(ExecuteNode(debug_output=True))

    def test_script_defaults(self):

        result=[]
        nodea=ExecuteNode(debug_output=True)
        nodea.script(lines=["echo test"], stdout_handler=lambda line: result.append(line))

        self.assertEqual(result, ["test"])

    def test_script_pipe(self):

        result=[]
        nodea=ExecuteNode()
        cmd_pipe=nodea.script(lines=["echo test"], pipe=True)
        nodea.script(lines=["tr e E"], inp=cmd_pipe,stdout_handler=lambda line: result.append(line))

        self.assertEqual(result, ["tEst"])


    def test_mixed(self):

        #should be able to mix run() and script()
        node=ExecuteNode()

        result=[]
        pipe=node.run(["echo", "test"], pipe=True)
        node.script(["tr e E"], inp=pipe, stdout_handler=lambda line: result.append(line))

        self.assertEqual(result, ["tEst"])






