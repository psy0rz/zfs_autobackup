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
        """parse arguments, setup logging, check and adjust parameters"""

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
        parser.add_argument('--ssh-config', metavar='CONFIG-FILE', default=None, help='Custom ssh client config')
        parser.add_argument('--ssh-source', metavar='USER@HOST', default=None,
                            help='Source host to get backup from.')
        parser.add_argument('--ssh-target', metavar='USER@HOST', default=None,
                            help='Target host to push backup to.')
        parser.add_argument('--keep-source', metavar='SCHEDULE', type=str, default="10,1d1w,1w1m,1m1y",
                            help='Thinning schedule for old source snapshots. Default: %(default)s')
        parser.add_argument('--keep-target', metavar='SCHEDULE', type=str, default="10,1d1w,1w1m,1m1y",
                            help='Thinning schedule for old target snapshots. Default: %(default)s')

        parser.add_argument('backup_name', metavar='BACKUP-NAME', default=None, nargs='?',
                            help='Name of the backup (you should set the zfs property "autobackup:backup-name" to '
                                 'true on filesystems you want to backup')
        parser.add_argument('target_path', metavar='TARGET-PATH', default=None, nargs='?',
                            help='Target ZFS filesystem (optional: if not specified, zfs-autobackup will only operate '
                                 'as snapshot-tool on source)')

        parser.add_argument('--pre-snapshot-cmd', metavar="COMMAND", default=[], action='append',
                            help='Run COMMAND before snapshotting (can be used multiple times.')
        parser.add_argument('--post-snapshot-cmd', metavar="COMMAND", default=[], action='append',
                            help='Run COMMAND after snapshotting (can be used multiple times.')
        parser.add_argument('--other-snapshots', action='store_true',
                            help='Send over other snapshots as well, not just the ones created by this tool.')
        parser.add_argument('--no-snapshot', action='store_true',
                            help='Don\'t create new snapshots (useful for finishing uncompleted backups, or cleanups)')
        parser.add_argument('--no-send', action='store_true',
                            help='Don\'t send snapshots (useful for cleanups, or if you want a serperate send-cronjob)')
        parser.add_argument('--no-thinning', action='store_true', help="Do not destroy any snapshots.")
        parser.add_argument('--no-holds', action='store_true',
                            help='Don\'t hold snapshots. (Faster. Allows you to destroy common snapshot.)')
        parser.add_argument('--min-change', metavar='BYTES', type=int, default=1,
                            help='Number of bytes written after which we consider a dataset changed (default %('
                                 'default)s)')
        parser.add_argument('--allow-empty', action='store_true',
                            help='If nothing has changed, still create empty snapshots. (same as --min-change=0)')

        parser.add_argument('--ignore-replicated', action='store_true', help=argparse.SUPPRESS)
        parser.add_argument('--exclude-unchanged', action='store_true',
                            help='Exclude datasets that have no changes since any last snapshot. (Useful in combination with proxmox HA replication)')
        parser.add_argument('--exclude-received', action='store_true',
                            help='Exclude datasets that have the origin of their autobackup: property as "received". '
                                 'This can avoid recursive replication between two backup partners.')
        parser.add_argument('--strip-path', metavar='N', default=0, type=int,
                            help='Number of directories to strip from target path (use 1 when cloning zones between 2 '
                                 'SmartOS machines)')

        parser.add_argument('--clear-refreservation', action='store_true',
                            help='Filter "refreservation" property. (recommended, safes space. same as '
                                 '--filter-properties refreservation)')
        parser.add_argument('--clear-mountpoint', action='store_true',
                            help='Set property canmount=noauto for new datasets. (recommended, prevents mount '
                                 'conflicts. same as --set-properties canmount=noauto)')
        parser.add_argument('--filter-properties', metavar='PROPERTY,...', type=str,
                            help='List of properties to "filter" when receiving filesystems. (you can still restore '
                                 'them with zfs inherit -S)')
        parser.add_argument('--set-properties', metavar='PROPERTY=VALUE,...', type=str,
                            help='List of propererties to override when receiving filesystems. (you can still restore '
                                 'them with zfs inherit -S)')
        parser.add_argument('--rollback', action='store_true',
                            help='Rollback changes to the latest target snapshot before starting. (normally you can '
                                 'prevent changes by setting the readonly property on the target_path to on)')
        parser.add_argument('--destroy-incompatible', action='store_true',
                            help='Destroy incompatible snapshots on target. Use with care! (implies --rollback)')
        parser.add_argument('--destroy-missing', metavar="SCHEDULE", type=str, default=None,
                            help='Destroy datasets on target that are missing on the source. Specify the time since '
                                 'the last snapshot, e.g: --destroy-missing 30d')
        parser.add_argument('--ignore-transfer-errors', action='store_true',
                            help='Ignore transfer errors (still checks if received filesystem exists. useful for '
                                 'acltype errors)')

        parser.add_argument('--decrypt', action='store_true',
                            help='Decrypt data before sending it over.')

        parser.add_argument('--encrypt', action='store_true',
                            help='Encrypt data after receiving it.')

        parser.add_argument('--zfs-compressed', action='store_true',
                            help='Transfer blocks that already have zfs-compression as-is.')

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

        parser.add_argument('--resume', action='store_true', help=argparse.SUPPRESS)
        parser.add_argument('--raw', action='store_true', help=argparse.SUPPRESS)

        # these things all do stuff by piping zfs send/recv IO
        parser.add_argument('--send-pipe', metavar="COMMAND", default=[], action='append',
                            help='pipe zfs send output through COMMAND (can be used multiple times)')
        parser.add_argument('--recv-pipe', metavar="COMMAND", default=[], action='append',
                            help='pipe zfs recv input through COMMAND (can be used multiple times)')
        parser.add_argument('--compress', metavar='TYPE', default=None, nargs='?', const='zstd-fast',
                            choices=compressors.choices(),
                            help='Use compression during transfer, defaults to zstd-fast if TYPE is not specified. ({})'.format(
                                ", ".join(compressors.choices())))
        parser.add_argument('--rate', metavar='DATARATE', default=None,
                            help='Limit data transfer rate (e.g. 128K. requires mbuffer.)')
        parser.add_argument('--buffer', metavar='SIZE', default=None,
                            help='Add zfs send and recv buffers to smooth out IO bursts. (e.g. 128M. requires mbuffer)')

        parser.add_argument('--snapshot-format', metavar='FORMAT', default="{}-%Y%m%d%H%M%S",
                            help='Snapshot naming format. Default: %(default)s')
        parser.add_argument('--property-format', metavar='FORMAT', default="autobackup:{}",
                            help='Select property naming format. Default: %(default)s')
        parser.add_argument('--hold-format', metavar='FORMAT', default="zfs_autobackup:{}",
                            help='Hold naming format. Default: %(default)s')

        parser.add_argument('--version', action='store_true',
                            help='Show version.')

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
