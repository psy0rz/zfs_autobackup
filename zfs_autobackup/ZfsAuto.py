import argparse
import os.path
import sys

from .LogConsole import LogConsole
from . import compressors


class ZfsAuto(object):
    """Common Base class for zfs-auto... tools """

    # also used by setup.py
    VERSION = "3.2-dev1"
    HEADER = "{} v{} - (c)2021 E.H.Eefting (edwin@datux.nl)".format(os.path.basename(sys.argv[0]), VERSION)

    def __init__(self, argv, print_arguments=True):

        # helps with investigating failed regression tests:
        if print_arguments:
            print("ARGUMENTS: " + " ".join(argv))

        self.args = self.parse_args(argv)

    def parse_args(self, argv):
        """parse common arguments, setup logging, check and adjust parameters"""

        parser=self.get_parser()
        args = parser.parse_args(argv)

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

        if args.allow_empty:
            args.min_change = 0

        if args.destroy_incompatible:
            args.rollback = True

        self.log = LogConsole(show_debug=args.debug, show_verbose=args.verbose, color=sys.stdout.isatty())

        self.verbose(self.HEADER)

        if args.backup_name == None:
            self.__parser.print_usage()
            self.log.error("Please specify BACKUP-NAME")
            sys.exit(255)

        if args.resume:
            self.warning("The --resume option isn't needed anymore (its autodetected now)")

        if args.raw:
            self.warning(
                "The --raw option isn't needed anymore (its autodetected now). Also see --encrypt and --decrypt.")

        if args.target_path is not None and args.target_path[0] == "/":
            self.log.error("Target should not start with a /")
            sys.exit(255)

        if args.compress and args.ssh_source is None and args.ssh_target is None:
            self.warning("Using compression, but transfer is local.")

        if args.compress and args.zfs_compressed:
            self.warning("Using --compress with --zfs-compressed, might be inefficient.")

        if args.ignore_replicated:
            self.warning("--ignore-replicated has been renamed, using --exclude-unchanged")
            args.exclude_unchanged = True

        return args

    def get_parser(self):

        parser = argparse.ArgumentParser(description=self.HEADER,
                                         epilog='Full manual at: https://github.com/psy0rz/zfs_autobackup')

        #positional arguments
        parser.add_argument('backup_name', metavar='BACKUP-NAME', default=None, nargs='?',
                            help='Name of the backup to select')

        parser.add_argument('target_path', metavar='TARGET-PATH', default=None, nargs='?',
                            help='Target ZFS filesystem (optional)')

        # Basic options
        parser.add_argument('--test', '--dry-run', '-n', action='store_true',
                            help='Dry run, dont change anything, just show what would be done (still does all read-only '
                                 'operations)')
        parser.add_argument('--verbose', '-v', action='store_true', help='verbose output')
        parser.add_argument('--debug', '-d', action='store_true',
                            help='Show zfs commands that are executed, stops after an exception.')
        parser.add_argument('--debug-output', action='store_true',
                            help='Show zfs commands and their output/exit codes. (noisy)')
        parser.add_argument('--progress', action='store_true',
                            help='show zfs progress output. Enabled automaticly on ttys. (use --no-progress to disable)')
        parser.add_argument('--no-progress', action='store_true',
                            help=argparse.SUPPRESS)  # needed to workaround a zfs recv -v bug
        parser.add_argument('--version', action='store_true',
                            help='Show version.')


        # SSH options
        group=parser.add_argument_group("SSH options")
        group.add_argument('--ssh-config', metavar='CONFIG-FILE', default=None, help='Custom ssh client config')
        group.add_argument('--ssh-source', metavar='USER@HOST', default=None,
                            help='Source host to get backup from.')
        group.add_argument('--ssh-target', metavar='USER@HOST', default=None,
                            help='Target host to push backup to.')



        return (parser)

    def verbose(self, txt):
        self.log.verbose(txt)

    def warning(self, txt):
        self.log.warning(txt)

    def error(self, txt):
        self.log.error(txt)

    def debug(self, txt):
        self.log.debug(txt)

    def set_title(self, title):
        self.log.verbose("")
        self.log.verbose("#### " + title)
