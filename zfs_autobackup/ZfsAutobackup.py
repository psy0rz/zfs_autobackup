import argparse
import sys
import time

from zfs_autobackup.Thinner import Thinner
from zfs_autobackup.ZfsDataset import ZfsDataset
from zfs_autobackup.LogConsole import LogConsole
from zfs_autobackup.ZfsNode import ZfsNode
from zfs_autobackup.ThinnerRule import ThinnerRule


class ZfsAutobackup:
    """main class"""

    VERSION = "3.0.1-beta8"
    HEADER = "zfs-autobackup v{} - Copyright 2020 E.H.Eefting (edwin@datux.nl)".format(VERSION)

    def __init__(self, argv, print_arguments=True):

        # helps with investigating failed regression tests:
        if print_arguments:
            print("ARGUMENTS: " + " ".join(argv))

        parser = argparse.ArgumentParser(
            description=self.HEADER,
            epilog='Full manual at: https://github.com/psy0rz/zfs_autobackup')
        parser.add_argument('--ssh-config', default=None, help='Custom ssh client config')
        parser.add_argument('--ssh-source', default=None,
                            help='Source host to get backup from. (user@hostname) Default %(default)s.')
        parser.add_argument('--ssh-target', default=None,
                            help='Target host to push backup to. (user@hostname) Default  %(default)s.')
        parser.add_argument('--keep-source', type=str, default="10,1d1w,1w1m,1m1y",
                            help='Thinning schedule for old source snapshots. Default: %(default)s')
        parser.add_argument('--keep-target', type=str, default="10,1d1w,1w1m,1m1y",
                            help='Thinning schedule for old target snapshots. Default: %(default)s')

        parser.add_argument('backup_name', metavar='backup-name',
                            help='Name of the backup (you should set the zfs property "autobackup:backup-name" to '
                                 'true on filesystems you want to backup')
        parser.add_argument('target_path', metavar='target-path', default=None, nargs='?',
                            help='Target ZFS filesystem (optional: if not specified, zfs-autobackup will only operate '
                                 'as snapshot-tool on source)')

        parser.add_argument('--other-snapshots', action='store_true',
                            help='Send over other snapshots as well, not just the ones created by this tool.')
        parser.add_argument('--no-snapshot', action='store_true',
                            help='Don\'t create new snapshots (useful for finishing uncompleted backups, or cleanups)')
        parser.add_argument('--no-send', action='store_true',
                            help='Don\'t send snapshots (useful for cleanups, or if you want a serperate send-cronjob)')
        #        parser.add_argument('--no-thinning', action='store_true', help='Don\'t run the thinner.')
        parser.add_argument('--min-change', type=int, default=1,
                            help='Number of bytes written after which we consider a dataset changed (default %('
                                 'default)s)')
        parser.add_argument('--allow-empty', action='store_true',
                            help='If nothing has changed, still create empty snapshots. (same as --min-change=0)')
        parser.add_argument('--ignore-replicated', action='store_true',
                            help='Ignore datasets that seem to be replicated some other way. (No changes since '
                                 'lastest snapshot. Useful for proxmox HA replication)')
        parser.add_argument('--no-holds', action='store_true',
                            help='Don\'t hold snapshots. (Faster)')

        parser.add_argument('--resume', action='store_true', help=argparse.SUPPRESS)
        parser.add_argument('--strip-path', default=0, type=int,
                            help='Number of directories to strip from target path (use 1 when cloning zones between 2 '
                                 'SmartOS machines)')
        # parser.add_argument('--buffer', default="",  help='Use mbuffer with specified size to speedup zfs transfer.
        # (e.g. --buffer 1G) Will also show nice progress output.')

        parser.add_argument('--clear-refreservation', action='store_true',
                            help='Filter "refreservation" property. (recommended, safes space. same as '
                                 '--filter-properties refreservation)')
        parser.add_argument('--clear-mountpoint', action='store_true',
                            help='Set property canmount=noauto for new datasets. (recommended, prevents mount '
                                 'conflicts. same as --set-properties canmount=noauto)')
        parser.add_argument('--filter-properties', type=str,
                            help='List of properties to "filter" when receiving filesystems. (you can still restore '
                                 'them with zfs inherit -S)')
        parser.add_argument('--set-properties', type=str,
                            help='List of propererties to override when receiving filesystems. (you can still restore '
                                 'them with zfs inherit -S)')
        parser.add_argument('--rollback', action='store_true',
                            help='Rollback changes to the latest target snapshot before starting. (normally you can '
                                 'prevent changes by setting the readonly property on the target_path to on)')
        parser.add_argument('--destroy-incompatible', action='store_true',
                            help='Destroy incompatible snapshots on target. Use with care! (implies --rollback)')
        parser.add_argument('--destroy-missing', type=str, default=None,
                            help='Destroy datasets on target that are missing on the source. Specify the time since '
                                 'the last snapshot, e.g: --destroy-missing 30d')
        parser.add_argument('--ignore-transfer-errors', action='store_true',
                            help='Ignore transfer errors (still checks if received filesystem exists. useful for '
                                 'acltype errors)')
        parser.add_argument('--raw', action='store_true',
                            help='For encrypted datasets, send data exactly as it exists on disk.')

        parser.add_argument('--test', action='store_true',
                            help='dont change anything, just show what would be done (still does all read-only '
                                 'operations)')
        parser.add_argument('--verbose', action='store_true', help='verbose output')
        parser.add_argument('--debug', action='store_true',
                            help='Show zfs commands that are executed, stops after an exception.')
        parser.add_argument('--debug-output', action='store_true',
                            help='Show zfs commands and their output/exit codes. (noisy)')
        parser.add_argument('--progress', action='store_true',
                            help='show zfs progress output. Enabled automaticly on ttys. (use --no-progress to disable)')
        parser.add_argument('--no-progress', action='store_true', help=argparse.SUPPRESS) #needed to workaround a zfs recv -v bug

        # note args is the only global variable we use, since its a global readonly setting anyway
        args = parser.parse_args(argv)

        self.args = args

        #auto enable progress?
        if sys.stderr.isatty() and not args.no_progress:
            args.progress = True

        if args.debug_output:
            args.debug = True

        if self.args.test:
            self.args.verbose = True

        if args.allow_empty:
            args.min_change = 0

        if args.destroy_incompatible:
            args.rollback = True

        self.log = LogConsole(show_debug=self.args.debug, show_verbose=self.args.verbose)

        if args.resume:
            self.verbose("NOTE: The --resume option isn't needed anymore (its autodetected now)")

        if args.target_path is not None and args.target_path[0] == "/":
            self.log.error("Target should not start with a /")
            sys.exit(255)

    def verbose(self, txt):
        self.log.verbose(txt)

    def error(self, txt):
        self.log.error(txt)

    def debug(self, txt):
        self.log.debug(txt)

    def set_title(self, title):
        self.log.verbose("")
        self.log.verbose("#### " + title)

    # sync datasets, or thin-only on both sides
    # target is needed for this.
    def sync_datasets(self, source_node, source_datasets):

        description = "[Target]"

        self.set_title("Target settings")

        target_thinner = Thinner(self.args.keep_target)
        target_node = ZfsNode(self.args.backup_name, self, ssh_config=self.args.ssh_config, ssh_to=self.args.ssh_target,
                              readonly=self.args.test, debug_output=self.args.debug_output, description=description,
                              thinner=target_thinner)
        target_node.verbose("Receive datasets under: {}".format(self.args.target_path))

        if self.args.no_send:
            self.set_title("Thinning source and target")
        else:
            self.set_title("Sending and thinning")

        # check if exists, to prevent vague errors
        target_dataset = ZfsDataset(target_node, self.args.target_path)
        if not target_dataset.exists:
            self.error("Target path '{}' does not exist. Please create this dataset first.".format(target_dataset))
            return 255

        if self.args.filter_properties:
            filter_properties = self.args.filter_properties.split(",")
        else:
            filter_properties = []

        if self.args.set_properties:
            set_properties = self.args.set_properties.split(",")
        else:
            set_properties = []

        if self.args.clear_refreservation:
            filter_properties.append("refreservation")

        if self.args.clear_mountpoint:
            set_properties.append("canmount=noauto")

        # sync datasets
        fail_count = 0
        target_datasets = []
        for source_dataset in source_datasets:

            try:
                # determine corresponding target_dataset
                target_name = self.args.target_path + "/" + source_dataset.lstrip_path(self.args.strip_path)
                target_dataset = ZfsDataset(target_node, target_name)
                target_datasets.append(target_dataset)

                # ensure parents exists
                # TODO: this isnt perfect yet, in some cases it can create parents when it shouldn't.
                if not self.args.no_send \
                        and target_dataset.parent not in target_datasets \
                        and not target_dataset.parent.exists:
                    target_dataset.parent.create_filesystem(parents=True)

                # determine common zpool features
                source_features = source_node.get_zfs_pool(source_dataset.split_path()[0]).features
                target_features = target_node.get_zfs_pool(target_dataset.split_path()[0]).features
                common_features = source_features and target_features
                # source_dataset.debug("Common features: {}".format(common_features))

                source_dataset.sync_snapshots(target_dataset, show_progress=self.args.progress,
                                              features=common_features, filter_properties=filter_properties,
                                              set_properties=set_properties,
                                              ignore_recv_exit_code=self.args.ignore_transfer_errors,
                                              holds=not self.args.no_holds, rollback=self.args.rollback,
                                              raw=self.args.raw, other_snapshots=self.args.other_snapshots,
                                              no_send=self.args.no_send,
                                              destroy_incompatible=self.args.destroy_incompatible)
            except Exception as e:
                fail_count = fail_count + 1
                source_dataset.error("FAILED: " + str(e))
                if self.args.debug:
                    raise

        # if not self.args.no_thinning:
        self.thin_missing_targets(ZfsDataset(target_node, self.args.target_path), target_datasets)

        return fail_count

    def thin_missing_targets(self, target_dataset, used_target_datasets):
        """thin/destroy target datasets that are missing on the source."""

        self.debug("Thinning obsolete datasets")

        for dataset in target_dataset.recursive_datasets:
            try:
                if dataset not in used_target_datasets:
                    dataset.debug("Missing on source, thinning")
                    dataset.thin()

                    # destroy_missing enabled?
                    if self.args.destroy_missing is not None:

                        # cant do anything without our own snapshots
                        if not dataset.our_snapshots:
                            if dataset.datasets:
                                dataset.debug("Destroy missing: ignoring")
                            else:
                                dataset.verbose(
                                    "Destroy missing: has no snapshots made by us. (please destroy manually)")
                        else:
                            # past the deadline?
                            deadline_ttl = ThinnerRule("0s" + self.args.destroy_missing).ttl
                            now = int(time.time())
                            if dataset.our_snapshots[-1].timestamp + deadline_ttl > now:
                                dataset.verbose("Destroy missing: Waiting for deadline.")
                            else:

                                dataset.debug("Destroy missing: Removing our snapshots.")

                                # remove all our snaphots, except last, to safe space in case we fail later on
                                for snapshot in dataset.our_snapshots[:-1]:
                                    snapshot.destroy(fail_exception=True)

                                # does it have other snapshots?
                                has_others = False
                                for snapshot in dataset.snapshots:
                                    if not snapshot.is_ours():
                                        has_others = True
                                        break

                                if has_others:
                                    dataset.verbose("Destroy missing: Still in use by other snapshots")
                                else:
                                    if dataset.datasets:
                                        dataset.verbose("Destroy missing: Still has children here.")
                                    else:
                                        dataset.verbose("Destroy missing.")
                                        dataset.our_snapshots[-1].destroy(fail_exception=True)
                                        dataset.destroy(fail_exception=True)

            except Exception as e:
                dataset.error("Error during destoy missing ({})".format(str(e)))

    def thin_source(self, source_datasets):

        self.set_title("Thinning source")

        for source_dataset in source_datasets:
            source_dataset.thin(skip_holds=True)

    def run(self):

        try:
            self.verbose(self.HEADER)

            if self.args.test:
                self.verbose("TEST MODE - SIMULATING WITHOUT MAKING ANY CHANGES")

            self.set_title("Source settings")

            description = "[Source]"
            source_thinner = Thinner(self.args.keep_source)
            source_node = ZfsNode(self.args.backup_name, self, ssh_config=self.args.ssh_config,
                                  ssh_to=self.args.ssh_source, readonly=self.args.test,
                                  debug_output=self.args.debug_output, description=description, thinner=source_thinner)
            source_node.verbose(
                "Selects all datasets that have property 'autobackup:{}=true' (or childs of datasets that have "
                "'autobackup:{}=child')".format(
                    self.args.backup_name, self.args.backup_name))

            self.set_title("Selecting")
            selected_source_datasets = source_node.selected_datasets
            if not selected_source_datasets:
                self.error(
                    "No source filesystems selected, please do a 'zfs set autobackup:{0}=true' on the source datasets "
                    "you want to select.".format(
                        self.args.backup_name))
                return 255

            source_datasets = []

            # filter out already replicated stuff?
            if not self.args.ignore_replicated:
                source_datasets = selected_source_datasets
            else:
                self.set_title("Filtering already replicated filesystems")
                for selected_source_dataset in selected_source_datasets:
                    if selected_source_dataset.is_changed(self.args.min_change):
                        source_datasets.append(selected_source_dataset)
                    else:
                        selected_source_dataset.verbose("Ignoring, already replicated")

            if not self.args.no_snapshot:
                self.set_title("Snapshotting")
                source_node.consistent_snapshot(source_datasets, source_node.new_snapshotname(),
                                                min_changed_bytes=self.args.min_change)

            # if target is specified, we sync the datasets, otherwise we just thin the source. (e.g. snapshot mode)
            if self.args.target_path:
                fail_count = self.sync_datasets(source_node, source_datasets)
            else:
                self.thin_source(source_datasets)
                fail_count = 0

            if not fail_count:
                if self.args.test:
                    self.set_title("All tests successfull.")
                else:
                    self.set_title("All operations completed successfully")
                    if not self.args.target_path:
                        self.verbose("(No target_path specified, only operated as snapshot tool.)")

            else:
                if fail_count != 255:
                    self.error("{} failures!".format(fail_count))

            if self.args.test:
                self.verbose("")
                self.verbose("TEST MODE - DID NOT MAKE ANY CHANGES!")

            return fail_count

        except Exception as e:
            self.error("Exception: " + str(e))
            if self.args.debug:
                raise
            return 255
        except KeyboardInterrupt:
            self.error("Aborted")
            return 255
