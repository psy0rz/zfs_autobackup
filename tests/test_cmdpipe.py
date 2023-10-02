from basetest import *
from zfs_autobackup.CmdPipe import CmdPipe,CmdItem


class TestCmdPipe(unittest2.TestCase):

    def test_single(self):
        """single process stdout and stderr"""
        p=CmdPipe(readonly=False, inp=None)
        err=[]
        out=[]
        p.add(CmdItem(["sh", "-c", "echo out1;echo err1 >&2; echo out2; echo err2 >&2"], stderr_handler=lambda line: err.append(line), exit_handler=lambda exit_code: self.assertEqual(exit_code,0), stdout_handler=lambda line: out.append(line)))
        executed=p.execute()

        self.assertEqual(out, ["out1", "out2"])
        self.assertEqual(err, ["err1","err2"])
        self.assertIsNone(executed)

    def test_input(self):
        """test stdinput"""
        p=CmdPipe(readonly=False, inp="test")
        err=[]
        out=[]
        p.add(CmdItem(["cat"], stderr_handler=lambda line: err.append(line), exit_handler=lambda exit_code: self.assertEqual(exit_code,0), stdout_handler=lambda line: out.append(line) ))
        executed=p.execute()

        self.assertEqual(err, [])
        self.assertEqual(out, ["test"])
        self.assertIsNone(executed)

    def test_pipe(self):
        """test piped"""
        p=CmdPipe(readonly=False)
        err1=[]
        err2=[]
        err3=[]
        out=[]
        p.add(CmdItem(["echo", "test"], stderr_handler=lambda line: err1.append(line), exit_handler=lambda exit_code: self.assertEqual(exit_code,0)))
        p.add(CmdItem(["tr", "e", "E"], stderr_handler=lambda line: err2.append(line), exit_handler=lambda exit_code: self.assertEqual(exit_code,0)))
        p.add(CmdItem(["tr", "t", "T"], stderr_handler=lambda line: err3.append(line), exit_handler=lambda exit_code: self.assertEqual(exit_code,0), stdout_handler=lambda line: out.append(line)))
        executed=p.execute()

        self.assertEqual(err1, [])
        self.assertEqual(err2, [])
        self.assertEqual(err3, [])
        self.assertEqual(out, ["TEsT"])
        self.assertIsNone(executed)

        #test str representation as well
        self.assertEqual(str(p), "(echo test) | (tr e E) | (tr t T)")

    def test_pipeerrors(self):
        """test piped stderrs """
        p=CmdPipe(readonly=False)
        err1=[]
        err2=[]
        err3=[]
        out=[]
        p.add(CmdItem(["sh", "-c", "echo err1 >&2"], stderr_handler=lambda line: err1.append(line), ))
        p.add(CmdItem(["sh", "-c", "echo err2 >&2"], stderr_handler=lambda line: err2.append(line), ))
        p.add(CmdItem(["sh", "-c", "echo err3 >&2"], stderr_handler=lambda line: err3.append(line), stdout_handler=lambda line: out.append(line)))
        executed=p.execute()

        self.assertEqual(err1, ["err1"])
        self.assertEqual(err2, ["err2"])
        self.assertEqual(err3, ["err3"])
        self.assertEqual(out, [])
        self.assertTrue(executed)

    def test_exitcode(self):
        """test piped exitcodes """
        p=CmdPipe(readonly=False)
        err1=[]
        err2=[]
        err3=[]
        out=[]
        p.add(CmdItem(["sh", "-c", "exit 1"], stderr_handler=lambda line: err1.append(line), exit_handler=lambda exit_code: self.assertEqual(exit_code,1)))
        p.add(CmdItem(["sh", "-c", "exit 2"], stderr_handler=lambda line: err2.append(line), exit_handler=lambda exit_code: self.assertEqual(exit_code,2)))
        p.add(CmdItem(["sh", "-c", "exit 3"], stderr_handler=lambda line: err3.append(line), exit_handler=lambda exit_code: self.assertEqual(exit_code,3), stdout_handler=lambda line: out.append(line)))
        executed=p.execute()

        self.assertEqual(err1, [])
        self.assertEqual(err2, [])
        self.assertEqual(err3, [])
        self.assertEqual(out, [])
        self.assertIsNone(executed)

    def test_readonly_execute(self):
        """everything readonly, just should execute"""

        p=CmdPipe(readonly=True)
        err1=[]
        err2=[]
        out=[]

        def true_exit(exit_code):
            return True

        p.add(CmdItem(["echo", "test1"], stderr_handler=lambda line: err1.append(line), exit_handler=true_exit, readonly=True))
        p.add(CmdItem(["echo", "test2"], stderr_handler=lambda line: err2.append(line), exit_handler=true_exit, readonly=True, stdout_handler=lambda line: out.append(line)))
        executed=p.execute()

        self.assertEqual(err1, [])
        self.assertEqual(err2, [])
        self.assertEqual(out, ["test2"])
        self.assertTrue(executed)

    def test_readonly_skip(self):
        """one command not readonly, skip"""

        p=CmdPipe(readonly=True)
        err1=[]
        err2=[]
        out=[]
        p.add(CmdItem(["echo", "test1"], stderr_handler=lambda line: err1.append(line), readonly=False))
        p.add(CmdItem(["echo", "test2"], stderr_handler=lambda line: err2.append(line), readonly=True, stdout_handler=lambda line: out.append(line)))
        executed=p.execute()

        self.assertEqual(err1, [])
        self.assertEqual(err2, [])
        self.assertEqual(out, [])
        self.assertTrue(executed)

    def test_no_handlers(self):
        with self.assertRaises(Exception):
            p=CmdPipe()
            p.add(CmdItem([ "echo" ]))
            p.execute()

        #NOTE: this will give some resource warnings

    def test_manual_pipes(self):

        # manual piping means: a command in the pipe has a stdout_handler, which is responsible for sending the data into the next item of the pipe.

        result=[]


        def stdout_handler(line):
            item2.process.stdin.write(line.encode('utf8'))

            # item2.process.stdin.close()

        item1=CmdItem(["echo", "test"], stdout_handler=stdout_handler)
        item2=CmdItem(["tr", "e", "E"], stdout_handler=lambda line: result.append(line))

        p=CmdPipe()
        p.add(item1)
        p.add(item2)
        p.execute()

        self.assertEqual(result, ["tEst"])

    def test_multiprocess(self):

        #dont do any piping at all, just run multiple processes and handle outputs

        result1=[]
        result2=[]
        result3=[]

        item1=CmdItem(["echo", "test1"], stdout_handler=lambda line: result1.append(line))
        item2=CmdItem(["echo", "test2"], stdout_handler=lambda line: result2.append(line))
        item3=CmdItem(["echo", "test3"], stdout_handler=lambda line: result3.append(line))

        p=CmdPipe()
        p.add(item1)
        p.add(item2)
        p.add(item3)
        p.execute()

        self.assertEqual(result1, ["test1"])
        self.assertEqual(result2, ["test2"])
        self.assertEqual(result3, ["test3"])

