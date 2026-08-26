from zfs_autobackup.LogConsole import LogConsole
from basetest import *


class TestLog(unittest2.TestCase):

    def test_colored(self):
        """test with color output"""
        with OutputIO() as buf:
            with redirect_stdout(buf):
                l= LogConsole(show_verbose=False, show_debug=False, color=True)
                l.verbose("verbose")
                l.debug("debug")

            with redirect_stdout(buf):
                l=LogConsole(show_verbose=True, show_debug=True, color=True)
                l.verbose("verbose")
                l.debug("debug")

            with redirect_stderr(buf):
                l=LogConsole(show_verbose=False, show_debug=False, color=True)
                l.error("error")

            print(list(buf.getvalue()))
            self.assertEqual(list(buf.getvalue()), ['\x1b', '[', '2', '2', 'm', ' ', ' ', 'v', 'e', 'r', 'b', 'o', 's', 'e', '\x1b', '[', '0', 'm', '\n', '\x1b', '[', '3', '2', 'm', '#', ' ', 'd', 'e', 'b', 'u', 'g', '\x1b', '[', '0', 'm', '\n', '\x1b', '[', '3', '1', 'm', '\x1b', '[', '1', 'm', '!', ' ', 'e', 'r', 'r', 'o', 'r', '\x1b', '[', '0', 'm', '\n'])

    def test_progress_nocolor_no_crash(self):
        """progress() + clear_progress() must not crash when color=False.

        Regression: clear_progress() used to do an unconditional `import colorama`
        and call colorama.ansi.clear_line(), which fails if colorama isn't installed.
        """

        with OutputIO() as buf:
            with redirect_stderr(buf):
                l = LogConsole(show_verbose=False, show_debug=False, color=False)
                l.progress("working")
                l.progress("still working")  # triggers clear_progress on the previous line
                l.clear_progress()
                # write something visible after clearing
                l.error("done")

            self.assertIn("done", buf.getvalue())

    def test_nocolor(self):
        """test without color output"""

        with OutputIO() as buf:
            with redirect_stdout(buf):
                l=LogConsole(show_verbose=False, show_debug=False, color=False)
                l.verbose("verbose")
                l.debug("debug")

            with redirect_stdout(buf):
                l=LogConsole(show_verbose=True, show_debug=True, color=False)
                l.verbose("verbose")
                l.debug("debug")

            with redirect_stderr(buf):
                l=LogConsole(show_verbose=False, show_debug=False, color=False)
                l.error("error")

            print(list(buf.getvalue()))
            self.assertEqual(list(buf.getvalue()), [' ', ' ', 'v', 'e', 'r', 'b', 'o', 's', 'e', '\n', '#', ' ', 'd', 'e', 'b', 'u', 'g', '\n', '!', ' ', 'e', 'r', 'r', 'o', 'r', '\n'])


        # zfs_autobackup.LogConsole.colorama=False



