from basetest import *
from zfs_autobackup.CmdPipe import CmdPipe,CmdItem


class TestCmdPipe(unittest2.TestCase):

    def test_single(self):
        """single process stdout and stderr"""
        p=CmdPipe(readonly=False, inp=None)
        err=[]
        out=[]
        p.add(CmdItem(["ls", "-d", "/", "/", "/nonexistent"], stderr_handler=lambda line: err.append(line), exit_handler=lambda exit_code: self.assertEqual(exit_code,2), stdout_handler=lambda line: out.append(line)))
        executed=p.execute()

        self.assertEqual(err, ["ls: cannot access '/nonexistent': No such file or directory"])
        self.assertEqual(out, ["/","/"])
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
        p.add(CmdItem(["ls", "/nonexistent1"], stderr_handler=lambda line: err1.append(line), exit_handler=lambda exit_code: self.assertEqual(exit_code,2)))
        p.add(CmdItem(["ls", "/nonexistent2"], stderr_handler=lambda line: err2.append(line), exit_handler=lambda exit_code: self.assertEqual(exit_code,2)))
        p.add(CmdItem(["ls", "/nonexistent3"], stderr_handler=lambda line: err3.append(line), exit_handler=lambda exit_code: self.assertEqual(exit_code,2), stdout_handler=lambda line: out.append(line)))
        executed=p.execute()

        self.assertEqual(err1, ["ls: cannot access '/nonexistent1': No such file or directory"])
        self.assertEqual(err2, ["ls: cannot access '/nonexistent2': No such file or directory"])
        self.assertEqual(err3, ["ls: cannot access '/nonexistent3': No such file or directory"])
        self.assertEqual(out, [])
        self.assertIsNone(executed)

    def test_exitcode(self):
        """test piped exitcodes """
        p=CmdPipe(readonly=False)
        err1=[]
        err2=[]
        err3=[]
        out=[]
        p.add(CmdItem(["bash", "-c", "exit 1"], stderr_handler=lambda line: err1.append(line), exit_handler=lambda exit_code: self.assertEqual(exit_code,1)))
        p.add(CmdItem(["bash", "-c", "exit 2"], stderr_handler=lambda line: err2.append(line), exit_handler=lambda exit_code: self.assertEqual(exit_code,2)))
        p.add(CmdItem(["bash", "-c", "exit 3"], stderr_handler=lambda line: err3.append(line), exit_handler=lambda exit_code: self.assertEqual(exit_code,3), stdout_handler=lambda line: out.append(line)))
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

