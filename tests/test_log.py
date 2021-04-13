import zfs_autobackup.LogConsole
from basetest import *


class TestLog(unittest2.TestCase):

    def test_colored(self):
        """test with color output"""
        with OutputIO() as buf:
            with redirect_stdout(buf):
                l=LogConsole(show_verbose=False, show_debug=False)
                l.verbose("verbose")
                l.debug("debug")

            with redirect_stdout(buf):
                l=LogConsole(show_verbose=True, show_debug=True)
                l.verbose("verbose")
                l.debug("debug")

            with redirect_stderr(buf):
                l=LogConsole()
                l.error("error")

            print(list(buf.getvalue()))
            self.assertEqual(list(buf.getvalue()), ['\x1b', '[', '2', '2', 'm', ' ', ' ', 'v', 'e', 'r', 'b', 'o', 's', 'e', '\x1b', '[', '0', 'm', '\n', '\x1b', '[', '3', '2', 'm', '#', ' ', 'd', 'e', 'b', 'u', 'g', '\x1b', '[', '0', 'm', '\n', '\x1b', '[', '3', '1', 'm', '\x1b', '[', '1', 'm', '!', ' ', 'e', 'r', 'r', 'o', 'r', '\x1b', '[', '0', 'm', '\n'])

    def test_nocolor(self):
        """test without color output"""

        zfs_autobackup.LogConsole.colorama=False

        with OutputIO() as buf:
            with redirect_stdout(buf):
                l=LogConsole(show_verbose=False, show_debug=False)
                l.verbose("verbose")
                l.debug("debug")

            with redirect_stdout(buf):
                l=LogConsole(show_verbose=True, show_debug=True)
                l.verbose("verbose")
                l.debug("debug")

            with redirect_stderr(buf):
                l=LogConsole()
                l.error("error")

            print(list(buf.getvalue()))
            self.assertEqual(list(buf.getvalue()), [' ', ' ', 'v', 'e', 'r', 'b', 'o', 's', 'e', '\n', '#', ' ', 'd', 'e', 'b', 'u', 'g', '\n', '!', ' ', 'e', 'r', 'r', 'o', 'r', '\n'])


        zfs_autobackup.LogConsole.colorama=False



