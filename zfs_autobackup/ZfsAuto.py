import argparse
import re
import sys

from .CliBase import CliBase
from .util import datetime_now


class ZfsAuto(CliBase):
    """Common Base class for ZfsAutobackup and ZfsAutoverify ."""

    def __init__(self, argv, print_arguments=True):

        self.hold_name = None
        self.snapshot_time_format = None
        self.property_name = None
        self.exclude_paths = None

        super(ZfsAuto, self).__init__(argv, print_arguments)

    def parse_args(self, argv):
        """parse common arguments, setup logging, check and adjust parameters"""

        args = super(ZfsAuto, self).parse_args(argv)

        if args.backup_name == None:
            self.parser.print_usage()
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
                if not args.exclude_received and not args.include_received:
                    self.verbose("NOTE: Source and target are on the same host, adding --exclude-received to commandline. (use --include-received to overrule)")
                    args.exclude_received = True

        if args.test:
            self.warning("TEST MODE - SIMULATING WITHOUT MAKING ANY CHANGES")

        #format all the names
        self.property_name = args.property_format.format(args.backup_name)
        self.snapshot_time_format = args.snapshot_format.format(args.backup_name)
        self.hold_name = args.hold_format.format(args.backup_name)

        dt = datetime_now(args.utc)

        self.verbose("")
        self.verbose("Current time {}           : {}".format(args.utc and "UTC" or "   ", dt.strftime("%Y-%m-%d %H:%M:%S")))

        self.verbose("Selecting dataset property : {}".format(self.property_name))
        self.verbose("Snapshot format            : {}".format(self.snapshot_time_format))
        self.verbose("Timezone                   : {}".format("UTC" if args.utc else "Local"))

        return args

    def get_parser(self):

        parser = super(ZfsAuto, self).get_parser()

        #positional arguments
        parser.add_argument('backup_name', metavar='BACKUP-NAME', default=None, nargs='?',
                            help='Name of the backup to select')

        parser.add_argument('target_path', metavar='TARGET-PATH', default=None, nargs='?',
                            help='Target ZFS filesystem (optional)')



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
        group.add_argument('--strip-path', metavar='N', default=0, type=int,
                           help='Number of directories to strip from target path.')

        group=parser.add_argument_group("Selection options")
        group.add_argument('--ignore-replicated', action='store_true', help=argparse.SUPPRESS)
        group.add_argument('--exclude-unchanged', metavar='BYTES', default=0, type=int,
                            help='Exclude datasets that have less than BYTES data changed since any last snapshot. (Use with proxmox HA replication)')
        group.add_argument('--exclude-received', action='store_true',
                            help='Exclude datasets that have the origin of their autobackup: property as "received". '
                                 'This can avoid recursive replication between two backup partners.')
        group.add_argument('--include-received', action='store_true',
                            help=argparse.SUPPRESS)


        def regex_argument_type(input_line):
            """Parses regex arguments into re.Pattern objects"""
            try:
                return re.compile(input_line)
            except:
                raise ValueError("Could not parse argument '{}' as a regular expression".format(input_line))
        group.add_argument('--exclude-snapshot-pattern', action='append', default=[], type=regex_argument_type, help="Regular expression to match snapshots that will be ignored.")

        return parser

    def print_error_sources(self):
        self.error(
            "No source filesystems selected, please do a 'zfs set autobackup:{0}=true' on the source datasets "
            "you want to select.".format(
                self.args.backup_name))

    def make_target_name(self, source_dataset):
        """make target_name from a source_dataset"""
        return self.args.target_path + "/" + source_dataset.lstrip_path(self.args.strip_path)
