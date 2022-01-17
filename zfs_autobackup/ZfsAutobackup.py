import time

import argparse
from .ZfsAuto import ZfsAuto

from . import compressors
from .ExecuteNode import ExecuteNode
from .Thinner import Thinner
from .ZfsDataset import ZfsDataset
from .ZfsNode import ZfsNode
from .ThinnerRule import ThinnerRule


class ZfsAutobackup(ZfsAuto):
    """The main zfs-autobackup class. Start here, at run() :)"""


    def __init__(self, argv, print_arguments=True):
        super(ZfsAutobackup, self).__init__(argv, print_arguments)

    def parse_args(self, argv):
        args=super(ZfsAutobackup, self).parse_args(argv)

        if args.allow_empty:
            args.min_change = 0

        if args.destroy_incompatible:
            args.rollback = True

        if args.resume:
            self.warning("The --resume option isn't needed anymore (its autodetected now)")

        if args.raw:
            self.warning(
                "The --raw option isn't needed anymore (its autodetected now). Also see --encrypt and --decrypt.")

        if args.compress and args.ssh_source is None and args.ssh_target is None:
            self.warning("Using compression, but transfer is local.")

        if args.compress and args.zfs_compressed:
            self.warning("Using --compress with --zfs-compressed, might be inefficient.")

        return args

    def get_parser(self):
        """extend common parser with  extra stuff needed for zfs-autobackup"""

        parser=super(ZfsAutobackup, self).get_parser()

        group=parser.add_argument_group("Snapshot options")
        group.add_argument('--no-snapshot', action='store_true',
                            help='Don\'t create new snapshots (useful for finishing uncompleted backups, or cleanups)')
        group.add_argument('--pre-snapshot-cmd', metavar="COMMAND", default=[], action='append',
                            help='Run COMMAND before snapshotting (can be used multiple times.')
        group.add_argument('--post-snapshot-cmd', metavar="COMMAND", default=[], action='append',
                            help='Run COMMAND after snapshotting (can be used multiple times.')
        group.add_argument('--min-change', metavar='BYTES', type=int, default=1,
                            help='Only create snapshot if enough bytes are changed. (default %('
                                 'default)s)')
        group.add_argument('--allow-empty', action='store_true',
                            help='If nothing has changed, still create empty snapshots. (Faster. Same as --min-change=0)')
        group.add_argument('--other-snapshots', action='store_true',
                            help='Send over other snapshots as well, not just the ones created by this tool.')
        group.add_argument('--snapshot-format', metavar='FORMAT', default="{}-%Y%m%d%H%M%S",
                            help='ZFS Snapshot string format. Default: %(default)s')

        group=parser.add_argument_group("Transfer options")
        group.add_argument('--no-send', action='store_true',
                            help='Don\'t transfer snapshots (useful for cleanups, or if you want a serperate send-cronjob)')
        group.add_argument('--no-holds', action='store_true',
                            help='Don\'t hold snapshots. (Faster. Allows you to destroy common snapshot.)')
        group.add_argument('--strip-path', metavar='N', default=0, type=int,
                            help='Number of directories to strip from target path (use 1 when cloning zones between 2 '
                                 'SmartOS machines)')
        group.add_argument('--clear-refreservation', action='store_true',
                            help='Filter "refreservation" property. (recommended, safes space. same as '
                                 '--filter-properties refreservation)')
        group.add_argument('--clear-mountpoint', action='store_true',
                            help='Set property canmount=noauto for new datasets. (recommended, prevents mount '
                                 'conflicts. same as --set-properties canmount=noauto)')
        group.add_argument('--filter-properties', metavar='PROPERTY,...', type=str,
                            help='List of properties to "filter" when receiving filesystems. (you can still restore '
                                 'them with zfs inherit -S)')
        group.add_argument('--set-properties', metavar='PROPERTY=VALUE,...', type=str,
                            help='List of propererties to override when receiving filesystems. (you can still restore '
                                 'them with zfs inherit -S)')
        group.add_argument('--rollback', action='store_true',
                            help='Rollback changes to the latest target snapshot before starting. (normally you can '
                                 'prevent changes by setting the readonly property on the target_path to on)')
        group.add_argument('--destroy-incompatible', action='store_true',
                            help='Destroy incompatible snapshots on target. Use with care! (implies --rollback)')
        group.add_argument('--ignore-transfer-errors', action='store_true',
                            help='Ignore transfer errors (still checks if received filesystem exists. useful for '
                                 'acltype errors)')

        group.add_argument('--decrypt', action='store_true',
                            help='Decrypt data before sending it over.')
        group.add_argument('--encrypt', action='store_true',
                            help='Encrypt data after receiving it.')

        group.add_argument('--zfs-compressed', action='store_true',
                            help='Transfer blocks that already have zfs-compression as-is.')
        group.add_argument('--hold-format', metavar='FORMAT', default="zfs_autobackup:{}",
                            help='ZFS hold string format. Default: %(default)s')



        group=parser.add_argument_group("ZFS send/recv pipes")
        group.add_argument('--compress', metavar='TYPE', default=None, nargs='?', const='zstd-fast',
                            choices=compressors.choices(),
                            help='Use compression during transfer, defaults to zstd-fast if TYPE is not specified. ({})'.format(
                                ", ".join(compressors.choices())))
        group.add_argument('--rate', metavar='DATARATE', default=None,
                            help='Limit data transfer rate (e.g. 128K. requires mbuffer.)')
        group.add_argument('--buffer', metavar='SIZE', default=None,
                            help='Add zfs send and recv buffers to smooth out IO bursts. (e.g. 128M. requires mbuffer)')
        group.add_argument('--send-pipe', metavar="COMMAND", default=[], action='append',
                            help='pipe zfs send output through COMMAND (can be used multiple times)')
        group.add_argument('--recv-pipe', metavar="COMMAND", default=[], action='append',
                            help='pipe zfs recv input through COMMAND (can be used multiple times)')


        group=parser.add_argument_group("Thinner options")
        group.add_argument('--no-thinning', action='store_true', help="Do not destroy any snapshots.")
        group.add_argument('--keep-source', metavar='SCHEDULE', type=str, default="10,1d1w,1w1m,1m1y",
                            help='Thinning schedule for old source snapshots. Default: %(default)s')
        group.add_argument('--keep-target', metavar='SCHEDULE', type=str, default="10,1d1w,1w1m,1m1y",
                            help='Thinning schedule for old target snapshots. Default: %(default)s')
        group.add_argument('--destroy-missing', metavar="SCHEDULE", type=str, default=None,
                            help='Destroy datasets on target that are missing on the source. Specify the time since '
                                 'the last snapshot, e.g: --destroy-missing 30d')


        #obsolete
        parser.add_argument('--resume', action='store_true', help=argparse.SUPPRESS)
        parser.add_argument('--raw', action='store_true', help=argparse.SUPPRESS)




        return (parser)

    def progress(self, txt):
        self.log.progress(txt)

    def clear_progress(self):
        self.log.clear_progress()

    # NOTE: this method also uses self.args. args that need extra processing are passed as function parameters:
    def thin_missing_targets(self, target_dataset, used_target_datasets):
        """thin target datasets that are missing on the source."""

        self.debug("Thinning obsolete datasets")
        missing_datasets = [dataset for dataset in target_dataset.recursive_datasets if
                            dataset not in used_target_datasets]

        count = 0
        for dataset in missing_datasets:

            count = count + 1
            if self.args.progress:
                self.progress("Analysing missing {}/{}".format(count, len(missing_datasets)))

            try:
                dataset.debug("Missing on source, thinning")
                dataset.thin()

            except Exception as e:
                dataset.error("Error during thinning of missing datasets ({})".format(str(e)))

        if self.args.progress:
            self.clear_progress()

    # NOTE: this method also uses self.args. args that need extra processing are passed as function parameters:
    def destroy_missing_targets(self, target_dataset, used_target_datasets):
        """destroy target datasets that are missing on the source and that meet the requirements"""

        self.debug("Destroying obsolete datasets")

        missing_datasets = [dataset for dataset in target_dataset.recursive_datasets if
                            dataset not in used_target_datasets]

        count = 0
        for dataset in missing_datasets:

            count = count + 1
            if self.args.progress:
                self.progress("Analysing destroy missing {}/{}".format(count, len(missing_datasets)))

            try:
                # cant do anything without our own snapshots
                if not dataset.our_snapshots:
                    if dataset.datasets:
                        # its not a leaf, just ignore
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
                dataset.error("Error during --destroy-missing: {}".format(str(e)))

        if self.args.progress:
            self.clear_progress()

    def get_send_pipes(self, logger):
        """determine the zfs send pipe"""

        ret = []

        # IO buffer
        if self.args.buffer:
            logger("zfs send buffer        : {}".format(self.args.buffer))
            ret.extend([ExecuteNode.PIPE, "mbuffer", "-q", "-s128k", "-m" + self.args.buffer])

        # custom pipes
        for send_pipe in self.args.send_pipe:
            ret.append(ExecuteNode.PIPE)
            ret.extend(send_pipe.split(" "))
            logger("zfs send custom pipe   : {}".format(send_pipe))

        # compression
        if self.args.compress != None:
            ret.append(ExecuteNode.PIPE)
            cmd = compressors.compress_cmd(self.args.compress)
            ret.extend(cmd)
            logger("zfs send compression   : {}".format(" ".join(cmd)))

        # transfer rate
        if self.args.rate:
            logger("zfs send transfer rate : {}".format(self.args.rate))
            ret.extend([ExecuteNode.PIPE, "mbuffer", "-q", "-s128k", "-m16M", "-R" + self.args.rate])

        return ret

    def get_recv_pipes(self, logger):

        ret = []

        # decompression
        if self.args.compress != None:
            cmd = compressors.decompress_cmd(self.args.compress)
            ret.extend(cmd)
            ret.append(ExecuteNode.PIPE)
            logger("zfs recv decompression : {}".format(" ".join(cmd)))

        # custom pipes
        for recv_pipe in self.args.recv_pipe:
            ret.extend(recv_pipe.split(" "))
            ret.append(ExecuteNode.PIPE)
            logger("zfs recv custom pipe   : {}".format(recv_pipe))

        # IO buffer
        if self.args.buffer:
            # only add second buffer if its usefull. (e.g. non local transfer or other pipes active)
            if self.args.ssh_source != None or self.args.ssh_target != None or self.args.recv_pipe or self.args.send_pipe or self.args.compress != None:
                logger("zfs recv buffer        : {}".format(self.args.buffer))
                ret.extend(["mbuffer", "-q", "-s128k", "-m" + self.args.buffer, ExecuteNode.PIPE])

        return ret

    # NOTE: this method also uses self.args. args that need extra processing are passed as function parameters:
    def sync_datasets(self, source_node, source_datasets, target_node):
        """Sync datasets, or thin-only on both sides
        :type target_node: ZfsNode
        :type source_datasets: list of ZfsDataset
        :type source_node: ZfsNode
        """

        send_pipes = self.get_send_pipes(source_node.verbose)
        recv_pipes = self.get_recv_pipes(target_node.verbose)

        fail_count = 0
        count = 0
        target_datasets = []
        for source_dataset in source_datasets:

            # stats
            if self.args.progress:
                count = count + 1
                self.progress("Analysing dataset {}/{} ({} failed)".format(count, len(source_datasets), fail_count))

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

                # determine common zpool features (cached, so no problem we call it often)
                source_features = source_node.get_zfs_pool(source_dataset.split_path()[0]).features
                target_features = target_node.get_zfs_pool(target_dataset.split_path()[0]).features
                common_features = source_features and target_features

                # sync the snapshots of this dataset
                source_dataset.sync_snapshots(target_dataset, show_progress=self.args.progress,
                                              features=common_features, filter_properties=self.filter_properties_list(),
                                              set_properties=self.set_properties_list(),
                                              ignore_recv_exit_code=self.args.ignore_transfer_errors,
                                              holds=not self.args.no_holds, rollback=self.args.rollback,
                                              also_other_snapshots=self.args.other_snapshots,
                                              no_send=self.args.no_send,
                                              destroy_incompatible=self.args.destroy_incompatible,
                                              send_pipes=send_pipes, recv_pipes=recv_pipes,
                                              decrypt=self.args.decrypt, encrypt=self.args.encrypt,
                                              zfs_compressed=self.args.zfs_compressed)
            except Exception as e:
                fail_count = fail_count + 1
                source_dataset.error("FAILED: " + str(e))
                if self.args.debug:
                    raise

        if self.args.progress:
            self.clear_progress()

        target_path_dataset = ZfsDataset(target_node, self.args.target_path)
        if not self.args.no_thinning:
            self.thin_missing_targets(target_dataset=target_path_dataset, used_target_datasets=target_datasets)

        if self.args.destroy_missing is not None:
            self.destroy_missing_targets(target_dataset=target_path_dataset, used_target_datasets=target_datasets)

        return fail_count

    def thin_source(self, source_datasets):

        self.set_title("Thinning source")

        for source_dataset in source_datasets:
            source_dataset.thin(skip_holds=True)

    def filter_properties_list(self):

        if self.args.filter_properties:
            filter_properties = self.args.filter_properties.split(",")
        else:
            filter_properties = []

        if self.args.clear_refreservation:
            filter_properties.append("refreservation")

        return filter_properties

    def set_properties_list(self):

        if self.args.set_properties:
            set_properties = self.args.set_properties.split(",")
        else:
            set_properties = []

        if self.args.clear_mountpoint:
            set_properties.append("canmount=noauto")

        return set_properties

    def run(self):

        try:

            if self.args.test:
                self.warning("TEST MODE - SIMULATING WITHOUT MAKING ANY CHANGES")

            #format all the names
            property_name = self.args.property_format.format(self.args.backup_name)
            snapshot_time_format = self.args.snapshot_format.format(self.args.backup_name)
            hold_name = self.args.hold_format.format(self.args.backup_name)

            self.verbose("")
            self.verbose("Selecting dataset property : {}".format(property_name))
            self.verbose("Snapshot format            : {}".format(snapshot_time_format))

            if not self.args.no_holds:
                self.verbose("Hold name                  : {}".format(hold_name))


            ################ create source zfsNode
            self.set_title("Source settings")

            description = "[Source]"
            if self.args.no_thinning:
                source_thinner = None
            else:
                source_thinner = Thinner(self.args.keep_source)
            source_node = ZfsNode(snapshot_time_format=snapshot_time_format, hold_name=hold_name, logger=self, ssh_config=self.args.ssh_config,
                                  ssh_to=self.args.ssh_source, readonly=self.args.test,
                                  debug_output=self.args.debug_output, description=description, thinner=source_thinner)


            ################# select source datasets
            self.set_title("Selecting")

            # Note: Before version v3.1-beta5, we always used exclude_received. This was a problem if you wanted to
            # replicate an existing backup to another host and use the same backupname/snapshots. However, exclude_received
            # may still need to be used to explicitly exclude a backup with the 'received' source property to avoid accidental
            # recursive replication of a zvol that is currently being received in another session (as it will have changes).
            exclude_paths = []
            exclude_received = self.args.exclude_received
            if self.args.ssh_source == self.args.ssh_target:
                if self.args.target_path:
                    # target and source are the same, make sure to exclude target_path
                    self.verbose("NOTE: Source and target are on the same host, excluding target-path from selection.")
                    exclude_paths.append(self.args.target_path)
                else:
                    self.verbose("NOTE: Source and target are on the same host, excluding received datasets from selection.")
                    exclude_received = True

            source_datasets = source_node.selected_datasets(property_name=property_name,exclude_received=exclude_received,
                                                                     exclude_paths=exclude_paths,
                                                                     exclude_unchanged=self.args.exclude_unchanged,
                                                                     min_change=self.args.min_change)
            if not source_datasets:
                self.error(
                    "No source filesystems selected, please do a 'zfs set autobackup:{0}=true' on the source datasets "
                    "you want to select.".format(
                        self.args.backup_name))
                return 255

            ################# snapshotting
            if not self.args.no_snapshot:
                self.set_title("Snapshotting")
                snapshot_name=time.strftime(snapshot_time_format)
                source_node.consistent_snapshot(source_datasets, snapshot_name,
                                                min_changed_bytes=self.args.min_change,
                                                pre_snapshot_cmds=self.args.pre_snapshot_cmd,
                                                post_snapshot_cmds=self.args.post_snapshot_cmd)

            ################# sync
            # if target is specified, we sync the datasets, otherwise we just thin the source. (e.g. snapshot mode)
            if self.args.target_path:

                # create target_node
                self.set_title("Target settings")
                if self.args.no_thinning:
                    target_thinner = None
                else:
                    target_thinner = Thinner(self.args.keep_target)
                target_node = ZfsNode(snapshot_time_format=snapshot_time_format, hold_name=hold_name, logger=self, ssh_config=self.args.ssh_config,
                                      ssh_to=self.args.ssh_target,
                                      readonly=self.args.test, debug_output=self.args.debug_output,
                                      description="[Target]",
                                      thinner=target_thinner)
                target_node.verbose("Receive datasets under: {}".format(self.args.target_path))

                self.set_title("Synchronising")

                # check if exists, to prevent vague errors
                target_dataset = ZfsDataset(target_node, self.args.target_path)
                if not target_dataset.exists:
                    raise (Exception(
                        "Target path '{}' does not exist. Please create this dataset first.".format(target_dataset)))

                # do the actual sync
                # NOTE: even with no_send, no_thinning and no_snapshot it does a usefull thing because it checks if the common snapshots and shows incompatible snapshots
                fail_count = self.sync_datasets(
                    source_node=source_node,
                    source_datasets=source_datasets,
                    target_node=target_node)

            # no target specified, run in snapshot-only mode
            else:
                if not self.args.no_thinning:
                    self.thin_source(source_datasets)
                fail_count = 0

            if not fail_count:
                if self.args.test:
                    self.set_title("All tests successful.")
                else:
                    self.set_title("All operations completed successfully")
                    if not self.args.target_path:
                        self.verbose("(No target_path specified, only operated as snapshot tool.)")

            else:
                if fail_count != 255:
                    self.error("{} dataset(s) failed!".format(fail_count))

            if self.args.test:
                self.verbose("")
                self.warning("TEST MODE - DID NOT MAKE ANY CHANGES!")

            return fail_count

        except Exception as e:
            self.error("Exception: " + str(e))
            if self.args.debug:
                raise
            return 255
        except KeyboardInterrupt:
            self.error("Aborted")
            return 255
