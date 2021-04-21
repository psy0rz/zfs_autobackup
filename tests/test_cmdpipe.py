from basetest import *
from zfs_autobackup.CmdPipe import CmdPipe


class TestCmdPipe(unittest2.TestCase):

    def test_single(self):
        """single process stdout and stderr"""
        p=CmdPipe(readonly=False, inp=None)
        err=[]
        out=[]
        p.add(["ls", "-d", "/", "/", "/nonexistent"], stderr_handler=lambda line: err.append(line))
        executed=p.execute(stdout_handler=lambda line: out.append(line))

        self.assertEqual(err, ["ls: cannot access '/nonexistent': No such file or directory"])
        self.assertEqual(out, ["/","/"])
        self.assertTrue(executed)
        self.assertEqual(p.items[0]['process'].returncode,2)

    def test_input(self):
        """test stdinput"""
        p=CmdPipe(readonly=False, inp="test")
        err=[]
        out=[]
        p.add(["echo", "test"], stderr_handler=lambda line: err.append(line))
        executed=p.execute(stdout_handler=lambda line: out.append(line))

        self.assertEqual(err, [])
        self.assertEqual(out, ["test"])
        self.assertTrue(executed)
        self.assertEqual(p.items[0]['process'].returncode,0)

    def test_pipe(self):
        """test piped"""
        p=CmdPipe(readonly=False)
        err1=[]
        err2=[]
        err3=[]
        out=[]
        p.add(["echo", "test"], stderr_handler=lambda line: err1.append(line))
        p.add(["tr", "e", "E"], stderr_handler=lambda line: err2.append(line))
        p.add(["tr", "t", "T"], stderr_handler=lambda line: err3.append(line))
        executed=p.execute(stdout_handler=lambda line: out.append(line))

        self.assertEqual(err1, [])
        self.assertEqual(err2, [])
        self.assertEqual(err3, [])
        self.assertEqual(out, ["TEsT"])
        self.assertTrue(executed)
        self.assertEqual(p.items[0]['process'].returncode,0)
        self.assertEqual(p.items[1]['process'].returncode,0)
        self.assertEqual(p.items[2]['process'].returncode,0)

        #test str representation as well
        self.assertEqual(str(p), "(echo test) | (tr e E) | (tr t T)")

    def test_pipeerrors(self):
        """test piped stderrs """
        p=CmdPipe(readonly=False)
        err1=[]
        err2=[]
        err3=[]
        out=[]
        p.add(["ls", "/nonexistent1"], stderr_handler=lambda line: err1.append(line))
        p.add(["ls", "/nonexistent2"], stderr_handler=lambda line: err2.append(line))
        p.add(["ls", "/nonexistent3"], stderr_handler=lambda line: err3.append(line))
        executed=p.execute(stdout_handler=lambda line: out.append(line))

        self.assertEqual(err1, ["ls: cannot access '/nonexistent1': No such file or directory"])
        self.assertEqual(err2, ["ls: cannot access '/nonexistent2': No such file or directory"])
        self.assertEqual(err3, ["ls: cannot access '/nonexistent3': No such file or directory"])
        self.assertEqual(out, [])
        self.assertTrue(executed)
        self.assertEqual(p.items[0]['process'].returncode,2)
        self.assertEqual(p.items[1]['process'].returncode,2)
        self.assertEqual(p.items[2]['process'].returncode,2)

    def test_exitcode(self):
        """test piped exitcodes """
        p=CmdPipe(readonly=False)
        err1=[]
        err2=[]
        err3=[]
        out=[]
        p.add(["bash", "-c", "exit 1"], stderr_handler=lambda line: err1.append(line))
        p.add(["bash", "-c", "exit 2"], stderr_handler=lambda line: err2.append(line))
        p.add(["bash", "-c", "exit 3"], stderr_handler=lambda line: err3.append(line))
        executed=p.execute(stdout_handler=lambda line: out.append(line))

        self.assertEqual(err1, [])
        self.assertEqual(err2, [])
        self.assertEqual(err3, [])
        self.assertEqual(out, [])
        self.assertTrue(executed)
        self.assertEqual(p.items[0]['process'].returncode,1)
        self.assertEqual(p.items[1]['process'].returncode,2)
        self.assertEqual(p.items[2]['process'].returncode,3)

    def test_readonly_execute(self):
        """everything readonly, just should execute"""

        p=CmdPipe(readonly=True)
        err1=[]
        err2=[]
        out=[]
        p.add(["echo", "test1"], stderr_handler=lambda line: err1.append(line), readonly=True)
        p.add(["echo", "test2"], stderr_handler=lambda line: err2.append(line), readonly=True)
        executed=p.execute(stdout_handler=lambda line: out.append(line))

        self.assertEqual(err1, [])
        self.assertEqual(err2, [])
        self.assertEqual(out, ["test2"])
        self.assertTrue(executed)
        self.assertEqual(p.items[0]['process'].returncode,0)
        self.assertEqual(p.items[1]['process'].returncode,0)

    def test_readonly_skip(self):
        """one command not readonly, skip"""

        p=CmdPipe(readonly=True)
        err1=[]
        err2=[]
        out=[]
        p.add(["echo", "test1"], stderr_handler=lambda line: err1.append(line), readonly=False)
        p.add(["echo", "test2"], stderr_handler=lambda line: err2.append(line), readonly=True)
        executed=p.execute(stdout_handler=lambda line: out.append(line))

        self.assertEqual(err1, [])
        self.assertEqual(err2, [])
        self.assertEqual(out, [])
        self.assertFalse(executed)

