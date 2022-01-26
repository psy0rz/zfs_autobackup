import argparse
import os.path
import sys

from .LogConsole import LogConsole


class ZfsAuto(object):
    """Common Base class, this class is always used subclassed. Look at ZfsAutobackup and ZfsAutoverify ."""

    # also used by setup.py
    VERSION = "3.2-alpha2"
    HEADER = "{} v{} - (c)2021 E.H.Eefting (edwin@datux.nl)".format(os.path.basename(sys.argv[0]), VERSION)

    def __init__(self, argv, print_arguments=True):

        self.hold_name = None
        self.snapshot_time_format = None
        self.property_name = None
        self.exclude_paths = None

        # helps with investigating failed regression tests:
        if print_arguments:
            print("ARGUMENTS: " + " ".join(argv))

        self.args = self.parse_args(argv)

    def parse_args(self, argv):
        """parse common arguments, setup logging, check and adjust parameters"""

        parser=self.get_parser()
        args = parser.parse_args(argv)

        if args.help:
            parser.print_help()
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

        if args.backup_name == None:
            parser.print_usage()
            self.log.error("Please specify BACKUP-NAME")
            sys.exit(255)

        if args.target_path is not None and args.target_path[0] == "/":
            self.log.error("Target should not start with a /")
            sys.exit(255)

        if args.ignore_replicated:
            self.warning("--ignore-replicated has been renamed, using --exclude-unchanged")
            args.exclude_unchanged = True

        # Note: Before version v3.1-beta5, we always used exclude_received. This was a problem if you wanted to
        # replicate an existing backup to another host and use the same backupname/snapshots. However, exclude_received
        # may still need to be used to explicitly exclude a backup with the 'received' source property to avoid accidental
        # recursive replication of a zvol that is currently being received in another session (as it will have changes).

        self.exclude_paths = []
        if args.ssh_source == args.ssh_target:
            if args.target_path:
                # target and source are the same, make sure to exclude target_path
                self.verbose("NOTE: Source and target are on the same host, excluding target-path from selection.")
                self.exclude_paths.append(args.target_path)
            else:
                self.verbose("NOTE: Source and target are on the same host, excluding received datasets from selection.")
                args.exclude_received = True

        if args.test:
            self.warning("TEST MODE - SIMULATING WITHOUT MAKING ANY CHANGES")

        #format all the names
        self.property_name = args.property_format.format(args.backup_name)
        self.snapshot_time_format = args.snapshot_format.format(args.backup_name)
        self.hold_name = args.hold_format.format(args.backup_name)

        self.verbose("")
        self.verbose("Selecting dataset property : {}".format(self.property_name))
        self.verbose("Snapshot format            : {}".format(self.snapshot_time_format))

        return args

    def get_parser(self):

        parser = argparse.ArgumentParser(description=self.HEADER, add_help=False,
                                         epilog='Full manual at: https://github.com/psy0rz/zfs_autobackup')

        #positional arguments
        parser.add_argument('backup_name', metavar='BACKUP-NAME', default=None, nargs='?',
                            help='Name of the backup to select')

        parser.add_argument('target_path', metavar='TARGET-PATH', default=None, nargs='?',
                            help='Target ZFS filesystem (optional)')

        # Basic options
        group=parser.add_argument_group("Basic options")
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
        group.add_argument('--version', action='store_true',
                            help='Show version.')
        group.add_argument('--strip-path', metavar='N', default=0, type=int,
                           help='Number of directories to strip from target path (use 1 when cloning zones between 2 '
                                'SmartOS machines)')

        # SSH options
        group=parser.add_argument_group("SSH options")
        group.add_argument('--ssh-config', metavar='CONFIG-FILE', default=None, help='Custom ssh client config')
        group.add_argument('--ssh-source', metavar='USER@HOST', default=None,
                            help='Source host to pull backup from.')
        group.add_argument('--ssh-target', metavar='USER@HOST', default=None,
                            help='Target host to push backup to.')

        group=parser.add_argument_group("String formatting options")
        group.add_argument('--property-format', metavar='FORMAT', default="autobackup:{}",
                            help='Dataset selection string format. Default: %(default)s')
        group.add_argument('--snapshot-format', metavar='FORMAT', default="{}-%Y%m%d%H%M%S",
                            help='ZFS Snapshot string format. Default: %(default)s')
        group.add_argument('--hold-format', metavar='FORMAT', default="zfs_autobackup:{}",
                            help='ZFS hold string format. Default: %(default)s')

        group=parser.add_argument_group("Selection options")
        group.add_argument('--ignore-replicated', action='store_true', help=argparse.SUPPRESS)
        group.add_argument('--exclude-unchanged', action='store_true',
                            help='Exclude datasets that have no changes since any last snapshot. (Useful in combination with proxmox HA replication)')
        group.add_argument('--exclude-received', action='store_true',
                            help='Exclude datasets that have the origin of their autobackup: property as "received". '
                                 'This can avoid recursive replication between two backup partners.')

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

    def print_error_sources(self):
        self.error(
            "No source filesystems selected, please do a 'zfs set autobackup:{0}=true' on the source datasets "
            "you want to select.".format(
                self.args.backup_name))

    def make_target_name(self, source_dataset):
        """make target_name from a source_dataset"""
        return self.args.target_path + "/" + source_dataset.lstrip_path(self.args.strip_path)
