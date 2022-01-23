import os

from .ZfsAuto import ZfsAuto
from .ZfsDataset import ZfsDataset
from .ZfsNode import ZfsNode
import sys
import platform


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
        return parser

    def compare_trees(self , source_node, source_path, target_node, target_path):
        """recursively compare checksums in both trees"""

        #NOTE: perhaps support multiple compare methods/commands?

        #currently we use rsync for this.

        cmd = ["rsync", "-rcn", "--info=COPY,DEL,MISC,NAME,SYMSAFE", "--msgs2stderr", "--delete" ]

        #local
        if source_node.ssh_to is None and target_node.ssh_to is None:
            cmd.append("{}/".format(source_path))
            cmd.append("{}/".format(target_path))
            stdout, stderr = source_node.run(cmd, return_stderr=True)

        #source is local
        elif source_node.ssh_to is None and target_node.ssh_to is not None:
            cmd.append("{}/".format(source_path))
            cmd.append("{}:{}/".format(target_node.ssh_to, target_path))
            stdout, stderr = source_node.run(cmd, return_stderr=True)

        #target is local
        elif source_node.ssh_to is not None and target_node.ssh_to is None:
            cmd.append("{}:{}/".format(source_node.ssh_to, source_path))
            cmd.append("{}/".format(target_path))

            stdout, stderr=target_node.run(cmd, return_stderr=True)

        else:
            raise Exception("Source and target cant both be remote when using rsync to verify datasets")

        if stderr:
            raise Exception("Dataset verify failed, see above list for differences")

    def verify_filesystem(self, source_snapshot, source_mnt, target_snapshot, target_mnt):

        try:
            source_snapshot.mount(source_mnt)
            target_snapshot.mount(target_mnt)

            self.compare_trees(source_snapshot.zfs_node, source_mnt, target_snapshot.zfs_node, target_mnt)


        finally:
            source_snapshot.unmount()
            target_snapshot.unmount()

    def verify_volume(self, source_dataset, target_dataset):
        pass

    def verify_datasets(self, source_node, source_mnt, source_datasets, target_node, target_mnt):

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
                target_dataset = ZfsDataset(target_node, target_name)

                # find common snapshots to  verify
                source_snapshot = source_dataset.find_common_snapshot(target_dataset)
                target_snapshot = target_dataset.find_snapshot(source_snapshot)

                target_snapshot.verbose("Verifying...")

                if source_dataset.properties['type']=="filesystem":
                    self.verify_filesystem(source_snapshot, source_mnt, target_snapshot, target_mnt)
                elif source_dataset.properties['type']=="volume":
                    self.verify_volume(source_dataset, target_dataset)
                else:
                    raise(Exception("{} has unknown type {}".format(source_dataset, source_dataset.properties['type'])))


            except Exception as e:
                fail_count = fail_count + 1
                target_dataset.error("FAILED: " + str(e))
                if self.args.debug:
                    self.verbose("Debug mode, aborting on first error")
                    raise

        return fail_count

    def create_mountpoints(self, source_node, target_node):

        # prepare mount points
        source_node.debug("Create temporary mount point")
        source_mnt = "/tmp/zfs-autoverify_source_{}_{}".format(platform.node(), os.getpid())
        source_node.run(["mkdir", source_mnt])

        target_node.debug("Create temporary mount point")
        target_mnt = "/tmp/zfs-autoverify_target_{}_{}".format(platform.node(), os.getpid())
        target_node.run(["mkdir", target_mnt])

        return source_mnt, target_mnt

    def cleanup_mountpoint(self, node, mnt):
        node.debug("Cleaning up temporary mount point")
        node.run([ "rmdir", mnt ], hide_errors=True, valid_exitcodes=[] )


    def run(self):

        source_node=None
        source_mnt=None
        target_node=None
        target_mnt=None


        try:

            ################ create source zfsNode
            self.set_title("Source settings")

            description = "[Source]"
            source_node = ZfsNode(snapshot_time_format=self.snapshot_time_format, hold_name=self.hold_name, logger=self,
                                  ssh_config=self.args.ssh_config,
                                  ssh_to=self.args.ssh_source, readonly=self.args.test,
                                  debug_output=self.args.debug_output, description=description)

            ################# select source datasets
            self.set_title("Selecting")
            source_datasets = source_node.selected_datasets(property_name=self.property_name,
                                                            exclude_received=self.args.exclude_received,
                                                            exclude_paths=self.exclude_paths,
                                                            exclude_unchanged=self.args.exclude_unchanged,
                                                            min_change=0)
            if not source_datasets:
                self.print_error_sources()
                return 255

            # create target_node
            self.set_title("Target settings")
            target_node = ZfsNode(snapshot_time_format=self.snapshot_time_format, hold_name=self.hold_name,
                                  logger=self, ssh_config=self.args.ssh_config,
                                  ssh_to=self.args.ssh_target,
                                  readonly=self.args.test, debug_output=self.args.debug_output,
                                  description="[Target]")
            target_node.verbose("Verify datasets under: {}".format(self.args.target_path))

            self.set_title("Verifying")

            source_mnt, target_mnt=self.create_mountpoints(source_node, target_node)

            fail_count = self.verify_datasets(
                source_node=source_node,
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
                self.cleanup_mountpoint(source_node, source_mnt)

            if target_mnt is not None:
                self.cleanup_mountpoint(target_node, target_mnt)




def cli():
    import sys

    sys.exit(ZfsAutoverify(sys.argv[1:], False).run())


if __name__ == "__main__":
    cli()
