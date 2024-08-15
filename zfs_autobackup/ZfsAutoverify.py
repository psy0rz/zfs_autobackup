# from util import activate_volume_snapshot, create_mountpoints, cleanup_mountpoint
from signal import signal, SIGPIPE
from .util import output_redir, sigpipe_handler

from .ZfsAuto import ZfsAuto
from .ZfsNode import ZfsNode
import sys


# # try to be as unix compatible as possible, while still having decent performance
# def compare_trees_find(source_node, source_path, target_node, target_path):
#     # find /tmp/zfstmp_pve1_1993135target/ -xdev -type f -print0 | xargs -0 md5sum | md5sum -c
#
#     #verify tree has atleast one file
#
#     stdout=source_node.run(["find", ".", "-type", "f",
#                           ExecuteNode.PIPE, "head", "-n1",
#                           ], cwd=source_path)
#
#     if not stdout:
#         source_node.debug("No files, skipping check")
#     else:
#         pipe=source_node.run(["find", ".", "-type", "f", "-print0",
#                               ExecuteNode.PIPE, "xargs", "-0", "md5sum"
#                               ], pipe=True, cwd=source_path)
#         stdout=target_node.run([ "md5sum", "-c", "--quiet"], inp=pipe, cwd=target_path, valid_exitcodes=[0,1])
#
#         if len(stdout):
#             for line in stdout:
#                 target_node.error("md5sum: "+line)
#
#             raise(Exception("Some files have checksum errors"))
#
#
# def compare_trees_rsync(source_node, source_path, target_node, target_path):
#     """use rsync to compare two trees.
#      Advantage is that we can see which individual files differ.
#      But requires rsync and cant do remote to remote."""
#
#     cmd = ["rsync", "-rcnq", "--info=COPY,DEL,MISC,NAME,SYMSAFE", "--msgs2stderr", "--delete" ]
#
#     #local
#     if source_node.ssh_to is None and target_node.ssh_to is None:
#         cmd.append("{}/".format(source_path))
#         cmd.append("{}/".format(target_path))
#         source_node.debug("Running rsync locally, on source.")
#         stdout, stderr = source_node.run(cmd, return_stderr=True)
#
#     #source is local
#     elif source_node.ssh_to is None and target_node.ssh_to is not None:
#         cmd.append("{}/".format(source_path))
#         cmd.append("{}:{}/".format(target_node.ssh_to, target_path))
#         source_node.debug("Running rsync locally, on source.")
#         stdout, stderr = source_node.run(cmd, return_stderr=True)
#
#     #target is local
#     elif source_node.ssh_to is not None and target_node.ssh_to is None:
#         cmd.append("{}:{}/".format(source_node.ssh_to, source_path))
#         cmd.append("{}/".format(target_path))
#         source_node.debug("Running rsync locally, on target.")
#         stdout, stderr=target_node.run(cmd, return_stderr=True)
#
#     else:
#         raise Exception("Source and target cant both be remote when verifying. (rsync limitation)")
#
#     if stderr:
#         raise Exception("Dataset verify failed, see above list for differences")


def verify_filesystem(source_snapshot, source_mnt, target_snapshot, target_mnt, method):
    """Compare the contents of two zfs filesystem snapshots """

    try:

        # mount the snapshots
        source_snapshot.mount(source_mnt)
        target_snapshot.mount(target_mnt)

        if method=='rsync':
            compare_trees_rsync(source_snapshot.zfs_node, source_mnt, target_snapshot.zfs_node, target_mnt)
        # elif method == 'tar':
        #     compare_trees_tar(source_snapshot.zfs_node, source_mnt, target_snapshot.zfs_node, target_mnt)
        elif method == 'find':
            compare_trees_find(source_snapshot.zfs_node, source_mnt, target_snapshot.zfs_node, target_mnt)
        else:
            raise(Exception("program errror, unknown method"))

    finally:
        source_snapshot.unmount(source_mnt)
        target_snapshot.unmount(target_mnt)


# def hash_dev(node, dev):
#     """calculate md5sum of a device on a node"""
#
#     node.debug("Hashing volume {} ".format(dev))
#
#     cmd = [ "md5sum", dev ]
#
#     stdout = node.run(cmd)
#
#     if node.readonly:
#         hashed=None
#     else:
#         hashed = stdout[0].split(" ")[0]
#
#     node.debug("Hash of volume {} is {}".format(dev, hashed))
#
#     return hashed



# def deacitvate_volume_snapshot(snapshot):
#     clone_name=get_tmp_clone_name(snapshot)
#     clone=snapshot.zfs_node.get_dataset(clone_name)
#     clone.destroy(deferred=True, verbose=False)

def verify_volume(source_dataset, source_snapshot, target_dataset, target_snapshot):
    """compare the contents of two zfs volume snapshots"""

    # try:
    source_dev= activate_volume_snapshot(source_snapshot)
    target_dev= activate_volume_snapshot(target_snapshot)

    source_hash= hash_dev(source_snapshot.zfs_node, source_dev)
    target_hash= hash_dev(target_snapshot.zfs_node, target_dev)

    if source_hash!=target_hash:
        raise Exception("md5hash difference: {} != {}".format(source_hash, target_hash))

    # finally:
    #     deacitvate_volume_snapshot(source_snapshot)
    #     deacitvate_volume_snapshot(target_snapshot)


# class ZfsAutoChecksumVolume(ZfsAuto):
#     def __init__(self, argv, print_arguments=True):
#
#         # NOTE: common options and parameters are in ZfsAuto
#         super(ZfsAutoverify, self).__init__(argv, print_arguments)

class ZfsAutoverify(ZfsAuto):
    """The zfs-autoverify class, default agruments and stuff come from ZfsAuto"""

    def __init__(self, argv, print_arguments=True):

        # NOTE: common options and parameters are in ZfsAuto
        super(ZfsAutoverify, self).__init__(argv, print_arguments)

    def parse_args(self, argv):
        """do extra checks on common args"""

        args=super(ZfsAutoverify, self).parse_args(argv)

        if args.target_path == None:
            self.log.error("Please specify TARGET-PATH")
            sys.exit(255)

        return args

    def get_parser(self):
        """extend common parser with  extra stuff needed for zfs-autobackup"""

        parser=super(ZfsAutoverify, self).get_parser()

        group=parser.add_argument_group("Verify options")
        group.add_argument('--fs-compare', metavar='METHOD', default="find", choices=["find", "rsync"],
                            help='Compare method to use for filesystems. (find, rsync) Default: %(default)s ')

        return parser

    def verify_datasets(self, source_mnt, source_datasets, target_node, target_mnt):

        fail_count=0
        count = 0
        for source_dataset in source_datasets:

            # stats
            if self.args.progress:
                count = count + 1
                self.progress("Analysing dataset {}/{} ({} failed)".format(count, len(source_datasets), fail_count))

            try:
                # determine corresponding target_dataset
                target_name = self.make_target_name(source_dataset)
                target_dataset = target_node.get_dataset(target_name)

                # find common snapshots to  verify
                source_snapshot = source_dataset.find_common_snapshot(target_dataset, True)
                target_snapshot = target_dataset.find_snapshot(source_snapshot)

                if source_snapshot is None or target_snapshot is None:
                    raise(Exception("Cant find common snapshot"))

                target_snapshot.verbose("Verifying...")

                if source_dataset.properties['type']=="filesystem":
                    verify_filesystem(source_snapshot, source_mnt, target_snapshot, target_mnt, self.args.fs_compare)
                elif source_dataset.properties['type']=="volume":
                    verify_volume(source_dataset, source_snapshot, target_dataset, target_snapshot)
                else:
                    raise(Exception("{} has unknown type {}".format(source_dataset, source_dataset.properties['type'])))


            except Exception as e:
                # if self.args.progress:
                #     self.clear_progress()

                fail_count = fail_count + 1
                target_dataset.error("FAILED: " + str(e))
                if self.args.debug:
                    self.verbose("Debug mode, aborting on first error")
                    raise

        # if self.args.progress:
        #     self.clear_progress()

        return fail_count

    def run(self):

        source_node=None
        source_mnt=None
        target_node=None
        target_mnt=None


        try:

            ################ create source zfsNode
            self.set_title("Source settings")

            description = "[Source]"
            source_node = ZfsNode(utc=self.args.utc,
                                  snapshot_time_format=self.snapshot_time_format, hold_name=self.hold_name, logger=self,
                                  ssh_config=self.args.ssh_config,
                                  ssh_to=self.args.ssh_source, readonly=self.args.test,
                                  debug_output=self.args.debug_output, description=description,
                                  exclude_snapshot_patterns=self.args.exclude_snapshot_pattern)

            ################# select source datasets
            self.set_title("Selecting")
            ( source_datasets, excluded_datasets) = source_node.selected_datasets(property_name=self.property_name,
                                                            exclude_received=self.args.exclude_received,
                                                            exclude_paths=self.exclude_paths,
                                                            exclude_unchanged=self.args.exclude_unchanged)
            if not source_datasets and not excluded_datasets:
                self.print_error_sources()
                return 255

            # create target_node
            self.set_title("Target settings")
            target_node = ZfsNode(utc=self.args.utc,
                                  snapshot_time_format=self.snapshot_time_format, hold_name=self.hold_name,
                                  logger=self, ssh_config=self.args.ssh_config,
                                  ssh_to=self.args.ssh_target,
                                  readonly=self.args.test, debug_output=self.args.debug_output,
                                  description="[Target]")
            target_node.verbose("Verify datasets under: {}".format(self.args.target_path))

            self.set_title("Verifying")

            source_mnt, target_mnt= create_mountpoints(source_node, target_node)

            fail_count = self.verify_datasets(
                source_mnt=source_mnt,
                source_datasets=source_datasets,
                target_mnt=target_mnt,
                target_node=target_node)

            if not fail_count:
                if self.args.test:
                    self.set_title("All tests successful.")
                else:
                    self.set_title("All datasets verified ok")

            else:
                if fail_count != 255:
                    self.error("{} dataset(s) failed!".format(fail_count))

            if self.args.test:
                self.verbose("")
                self.warning("TEST MODE - DID NOT VERIFY ANYTHING!")

            return fail_count

        except Exception as e:
            self.error("Exception: " + str(e))
            if self.args.debug:
                raise
            return 255
        except KeyboardInterrupt:
            self.error("Aborted")
            return 255
        finally:

            # cleanup
            if source_mnt is not None:
                cleanup_mountpoint(source_node, source_mnt)

            if target_mnt is not None:
                cleanup_mountpoint(target_node, target_mnt)




def cli():
    import sys

    raise(Exception("This program is incomplete, dont use it yet."))
    signal(SIGPIPE, sigpipe_handler)
    failed = ZfsAutoverify(sys.argv[1:], False).run()
    sys.exit(min(failed,255))


if __name__ == "__main__":
    cli()
