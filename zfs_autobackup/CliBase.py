import argparse
import os.path
import sys

from .LogConsole import LogConsole


class CliBase(object):
    """Base class for all cli programs
    Overridden in subclasses that add stuff for the specific programs."""

    # also used by setup.py
    VERSION = "3.3"
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

        # Basic options
        group=parser.add_argument_group("Common options")
        group.add_argument('--help', '-h', action='store_true', help='show help')
        group.add_argument('--test', '--dry-run', '-n', action='store_true',
                            help='Dry run, dont change anything, just show what would be done (still does all read-only '
                                 'operations)')
        group.add_argument('--verbose', '-v', action='store_true', help='verbose output')
        group.add_argument('--debug', '-d', action='store_true',
                            help='Show zfs commands that are executed, stops after an exception.')
        group.add_argument('--debug-output', action='store_true',
                            help='Show zfs commands and their output/exit codes. (noisy)')
        group.add_argument('--progress', action='store_true',
                            help='show zfs progress output. Enabled automaticly on ttys. (use --no-progress to disable)')
        group.add_argument('--no-progress', action='store_true',
                            help=argparse.SUPPRESS)  # needed to workaround a zfs recv -v bug
        group.add_argument('--utc', action='store_true',
                            help='Use UTC instead of local time when dealing with timestamps for both formatting and parsing. To snapshot in an ISO 8601 compliant time format you may for example specify --snapshot-format "{}-%%Y-%%m-%%dT%%H:%%M:%%SZ". Changing this parameter after-the-fact (existing snapshots) will cause their timestamps to be interpreted as a different time than before.')
        group.add_argument('--version', action='store_true',
                            help='Show version.')


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
