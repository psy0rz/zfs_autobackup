# python 2 compatibility
from __future__ import print_function

import sys


colorama = False
if sys.stdout.isatty():
    try:
        import colorama
    except ImportError:
        colorama = False
        pass


class LogConsole:
    """Log-class that outputs to console, adding colors if needed"""

    def __init__(self, show_debug=False, show_verbose=False):
        self.last_log = ""
        self.show_debug = show_debug
        self.show_verbose = show_verbose

    @staticmethod
    def error(txt):
        if colorama:
            print(colorama.Fore.RED + colorama.Style.BRIGHT + "! " + txt + colorama.Style.RESET_ALL, file=sys.stderr)
        else:
            print("! " + txt, file=sys.stderr)
        sys.stderr.flush()

    def verbose(self, txt):
        if self.show_verbose:
            if colorama:
                print(colorama.Style.NORMAL + "  " + txt + colorama.Style.RESET_ALL)
            else:
                print("  " + txt)
            sys.stdout.flush()

    def debug(self, txt):
        if self.show_debug:
            if colorama:
                print(colorama.Fore.GREEN + "# " + txt + colorama.Style.RESET_ALL)
            else:
                print("# " + txt)
            sys.stdout.flush()
