# python 2 compatibility
from __future__ import print_function

import sys

class LogConsole:
    """Log-class that outputs to console, adding colors if needed"""

    def __init__(self, show_debug, show_verbose, color):
        self.last_log = ""
        self.show_debug = show_debug
        self.show_verbose = show_verbose

        if color:
            # try to use color, failback if colorama not available
            self.colorama=False
            try:
                import colorama
                global colorama
                self.colorama = True
            except ImportError:
                pass

        else:
            self.colorama=False

    def error(self, txt):
        if self.colorama:
            print(colorama.Fore.RED + colorama.Style.BRIGHT + "! " + txt + colorama.Style.RESET_ALL, file=sys.stderr)
        else:
            print("! " + txt, file=sys.stderr)
        sys.stderr.flush()

    def verbose(self, txt):
        if self.show_verbose:
            if self.colorama:
                print(colorama.Style.NORMAL + "  " + txt + colorama.Style.RESET_ALL)
            else:
                print("  " + txt)
            sys.stdout.flush()

    def debug(self, txt):
        if self.show_debug:
            if self.colorama:
                print(colorama.Fore.GREEN + "# " + txt + colorama.Style.RESET_ALL)
            else:
                print("# " + txt)
            sys.stdout.flush()
