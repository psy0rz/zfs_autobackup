from __future__ import print_function

import time
from signal import signal, SIGPIPE

from .TreeHasher import TreeHasher
from .BlockHasher import BlockHasher
from .ZfsNode import ZfsNode
from .util import *
from .CliBase import CliBase


class ZfsCheck(CliBase):

    def __init__(self, argv, print_arguments=True):

        # NOTE: common options argument parsing are in CliBase
        super(ZfsCheck, self).__init__(argv, print_arguments)

        self.node = ZfsNode(self.log, readonly=self.args.test, debug_output=self.args.debug_output)
        self.block_hasher = BlockHasher(count=self.args.count, bs=self.args.block_size)

    def get_parser(self):

        parser = super(ZfsCheck, self).get_parser()

        # positional arguments
        parser.add_argument('target', metavar='TARGET', default=None, nargs='?', help='Target to checksum. (can be blockdevice, directory or ZFS snapshot)')

        group = parser.add_argument_group('Hasher options')

        group.add_argument('--block-size', metavar="BYTES", default=4096, help="Read block-size, default %(default)s",
                           type=int)
        group.add_argument('--count', metavar="COUNT", default=int((100 * (1024 ** 2)) / 4096),
                           help="Hash chunks of COUNT blocks. Default %(default)s . (Chunk size is BYTES * COUNT) ", type=int)  # 100MiB

        group.add_argument('--check', '-c', metavar="FILE", default=None,
                           help="Read hashes from FILE and check them")

        return parser

    def parse_args(self, argv):
        args = super(ZfsCheck, self).parse_args(argv)

        if args.test:
            self.warning("TEST MODE - NOT DOING ANYTHING USEFULL")
            self.log.show_debug = True  # show at least what we would do

        if args.target is None:
            self.error("Please specify TARGET")
            sys.exit(1)

        return args

    def generate_zfs_filesystem(self, snapshot, input_generator):
        """ recursively hash all files in this snapshot, using block_hash_tree()

        :type snapshot: ZfsDataset.ZfsDataset
        """
        mnt = "/tmp/" + tmp_name()

        try:
            self.debug("Create temporary mount point {}".format(mnt))
            self.node.run(["mkdir", mnt])

            snapshot.mount(mnt)

            tree_hasher=TreeHasher(self.block_hasher)

            self.debug("Hashing tree: {}".format(mnt))
            if not self.args.test:
                if input_generator:
                    for i in tree_hasher.compare(mnt, input_generator):
                        yield i
                else:
                    for i in tree_hasher.generate(mnt):
                        yield i

        finally:
            snapshot.unmount()
            self.debug("Cleaning up temporary mount point")
            self.node.run(["rmdir", mnt], hide_errors=True, valid_exitcodes=[])

    # NOTE: https://www.google.com/search?q=Mount+Path+Limit+freebsd
    # Freebsd has limitations regarding path length, so we have to clone it so the part stays sort
    def activate_volume_snapshot(self, snapshot):
        """clone volume, waits and tries to findout /dev path to the volume, in a compatible way. (linux/freebsd/smartos)"""

        clone_name = get_tmp_clone_name(snapshot)
        clone = snapshot.clone(clone_name)

        # TODO: add smartos location to this list as well
        locations = [
            "/dev/zvol/" + clone_name
        ]

        clone.debug("Waiting for /dev entry to appear in: {}".format(locations))
        time.sleep(0.1)

        start_time = time.time()
        while time.time() - start_time < 10:
            for location in locations:
                if os.path.exists(location):
                    return location

                # fake it in testmode
                if self.args.test:
                    return location

            time.sleep(1)

        raise (Exception("Timeout while waiting for /dev entry to appear. (looking in: {})".format(locations)))

    def deacitvate_volume_snapshot(self, snapshot):
        """destroys temporary volume snapshot"""
        clone_name = get_tmp_clone_name(snapshot)
        clone = snapshot.zfs_node.get_dataset(clone_name)
        clone.destroy(deferred=True, verbose=False)

    def generate_zfs_volume(self, snapshot, input_generator):
        try:
            dev=self.activate_volume_snapshot(snapshot)

            self.debug("Hashing dev: {}".format(dev))
            if not self.args.test:
                if input_generator:
                    for i in self.block_hasher.compare(dev, input_generator):
                        yield i
                else:
                    for i in self.block_hasher.generate(dev):
                        yield i

        finally:
            self.deacitvate_volume_snapshot(snapshot)

    def generate_zfs_target(self, input_generator):
        """specified arget is a ZFS snapshot"""

        snapshot = self.node.get_dataset(self.args.target)
        if not snapshot.exists:
            snapshot.error("Snapshot not found")
            sys.exit(1)

        if not snapshot.is_snapshot:
            snapshot.error("Dataset should be a snapshot")
            sys.exit(1)

        dataset_type = snapshot.parent.properties['type']

        if dataset_type == 'volume':
            return self.generate_zfs_volume(snapshot, input_generator)
        elif dataset_type == 'filesystem':
            return self.generate_zfs_filesystem(snapshot, input_generator)
        else:
            raise Exception("huh?")

    def generate(self, input_generator=None):
        """generate checksums or compare (and generate error messages)"""

        if '@' in self.args.target:
            self.verbose("Assuming target {} is ZFS snapshot.".format(self.args.target))
            return self.generate_zfs_target(input_generator)
        elif os.path.isdir(self.args.target):
            self.verbose("Target {} is directory, checking recursively.".format(self.args.target))
            return self.check_path(input_generator)
        elif os.path.isfile(self.args.target):
            self.verbose("Target {} is single file or blockdevice.".format(self.args.target))

    def input_parser(self, file_name):
        """parse input lines and generate items to use in compare functions"""
        with open(file_name, 'r') as input_fh:
            for line in input_fh:
                i=line.rstrip().split("\t")
                #ignores lines without tabs
                if (len(i)>1):
                    yield i

    def run(self):

        try:
            #run as generator
            if self.args.check==None:
                for i in self.generate(input_generator=None):
                    if len(i)==3:
                        print("{}\t{}\t{}".format(*i))
                    else:
                        print("{}\t{}".format(*i))
                    sys.stdout.flush()
            #run as compare
            else:
                input_generator=self.input_parser(self.args.check)
                for i in self.generate(input_generator):
                        if len(i)==4:
                            (file_name, chunk_nr, compare_hexdigest, actual_hexdigest)=i
                            self.log.error("{}\t{}\t{}\t{}".format(file_name, chunk_nr, compare_hexdigest, actual_hexdigest))
                        else:
                            (chunk_nr, compare_hexdigest, actual_hexdigest) = i
                            self.log.error("{}\t{}\t{}".format(chunk_nr, compare_hexdigest, actual_hexdigest))

        except Exception as e:
            self.error("Exception: " + str(e))
            if self.args.debug:
                raise
            return 255
        except KeyboardInterrupt:
            self.error("Aborted")
            return 255

def cli():
    import sys
    signal(SIGPIPE, sigpipe_handler)

    sys.exit(ZfsCheck(sys.argv[1:], False).run())

if __name__ == "__main__":

    cli()