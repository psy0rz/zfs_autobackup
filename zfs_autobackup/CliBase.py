import argparse
import os.path
import sys

from .LogConsole import LogConsole


class CliBase(object):
    """Base class for all cli programs
    Overridden in subclasses that add stuff for the specific programs."""

    # also used by setup.py
    VERSION = "3.2-alpha2"
    HEADER = "{} v{} - (c)2022 E.H.Eefting (edwin@datux.nl)".format(os.path.basename(sys.argv[0]), VERSION)

    def __init__(self, argv, print_arguments=True):

        self.parser=self.get_parser()
        self.args = self.parse_args(argv)

        # helps with investigating failed regression tests:
        if print_arguments:
            print("ARGUMENTS: " + " ".join(argv))

    def parse_args(self, argv):
        """parses the arguments and does additional checks, might print warnings or notes
        Overridden in subclasses with extra checks.
        """

        args = self.parser.parse_args(argv)

        if args.help:
            self.parser.print_help()
            sys.exit(255)

        if args.version:
            print(self.HEADER)
            sys.exit(255)

        # auto enable progress?
        if sys.stderr.isatty() and not args.no_progress:
            args.progress = True

        if args.debug_output:
            args.debug = True

        if args.test:
            args.verbose = True

        if args.debug:
            args.verbose = True

        self.log = LogConsole(show_debug=args.debug, show_verbose=args.verbose, color=sys.stdout.isatty())

        self.verbose(self.HEADER)
        self.verbose("")

        return args

    def get_parser(self):
        """build up the argument parser
        Overridden in subclasses that add extra arguments
        """

        parser = argparse.ArgumentParser(description=self.HEADER, add_help=False,
                                         epilog='Full manual at: https://github.com/psy0rz/zfs_autobackup')

        return parser

    def verbose(self, txt):
        self.log.verbose(txt)

    def warning(self, txt):
        self.log.warning(txt)

    def error(self, txt):
        self.log.error(txt)

    def debug(self, txt):
        self.log.debug(txt)

    def progress(self, txt):
        self.log.progress(txt)

    def clear_progress(self):
        self.log.clear_progress()

    def set_title(self, title):
        self.log.verbose("")
        self.log.verbose("#### " + title)