from __future__ import print_function

import time
from signal import signal, SIGPIPE

from . import util
from .TreeHasher import TreeHasher
from .BlockHasher import BlockHasher
from .ZfsNode import ZfsNode
from .util import *
from .CliBase import CliBase


class ZfsCheck(CliBase):

    def __init__(self, argv, print_arguments=True):

        # NOTE: common options argument parsing are in CliBase
        super(ZfsCheck, self).__init__(argv, print_arguments)

        self.node = ZfsNode(self.log, utc=self.args.utc, readonly=self.args.test, debug_output=self.args.debug_output)

        self.block_hasher = BlockHasher(count=self.args.count, bs=self.args.block_size, skip=self.args.skip)

    def get_parser(self):

        parser = super(ZfsCheck, self).get_parser()

        # positional arguments
        parser.add_argument('target', metavar='TARGET', default=None, nargs='?', help='Target to checkum. (can be blockdevice, directory or ZFS snapshot)')

        group = parser.add_argument_group('Checker options')

        group.add_argument('--block-size', metavar="BYTES", default=4096, help="Read block-size, default %(default)s",
                           type=int)
        group.add_argument('--count', metavar="COUNT", default=int((100 * (1024 ** 2)) / 4096),
                           help="Hash chunks of COUNT blocks. Default %(default)s . (CHUNK size is BYTES * COUNT) ", type=int)  # 100MiB

        group.add_argument('--check', '-c', metavar="FILE", default=None, const=True, nargs='?',
                           help="Read hashes from STDIN (or FILE) and compare them")

        group.add_argument('--skip', '-s', metavar="NUMBER", default=0, type=int,
                           help="Skip this number of chunks after every hash. %(default)s")

        return parser

    def parse_args(self, argv):
        args = super(ZfsCheck, self).parse_args(argv)

        if args.test:
            self.warning("TEST MODE - WILL ONLY DO READ-ONLY STUFF")

        if args.target is None:
            self.error("Please specify TARGET")
            sys.exit(1)

        self.verbose("Target               : {}".format(args.target))
        self.verbose("Block size           : {} bytes".format(args.block_size))
        self.verbose("Block count          : {}".format(args.count))
        self.verbose("Effective chunk size : {} bytes".format(args.count*args.block_size))
        self.verbose("Skip chunk count     : {} (checks {:.2f}% of data)".format(args.skip, 100/(1+args.skip)))
        self.verbose("")


        return args

    def prepare_zfs_filesystem(self, snapshot):

        mnt = "/tmp/" + tmp_name()
        self.debug("Create temporary mount point {}".format(mnt))
        self.node.run(["mkdir", mnt])
        snapshot.mount(mnt)
        return mnt

    def cleanup_zfs_filesystem(self, snapshot):
        mnt = "/tmp/" + tmp_name()
        snapshot.unmount(mnt)
        self.debug("Cleaning up temporary mount point")
        self.node.run(["rmdir", mnt], hide_errors=True, valid_exitcodes=[])

    # NOTE: https://www.google.com/search?q=Mount+Path+Limit+freebsd
    # Freebsd has limitations regarding path length, so we have to clone it so the part stays sort
    def prepare_zfs_volume(self, snapshot):
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

        raise (Exception("Timeout while waiting for /dev entry to appear. (looking in: {}). Hint: did you forget to load the encryption key?".format(locations)))

    def cleanup_zfs_volume(self, snapshot):
        """destroys temporary volume snapshot"""
        clone_name = get_tmp_clone_name(snapshot)
        clone = snapshot.zfs_node.get_dataset(clone_name)
        clone.destroy(deferred=True, verbose=False)

    def generate_tree_hashes(self, prepared_target):

        tree_hasher = TreeHasher(self.block_hasher)
        self.debug("Hashing tree: {}".format(prepared_target))
        for i in tree_hasher.generate(prepared_target):
            yield i

    def generate_tree_compare(self, prepared_target, input_generator=None):

        tree_hasher = TreeHasher(self.block_hasher)
        self.debug("Comparing tree: {}".format(prepared_target))
        for i in tree_hasher.compare(prepared_target, input_generator):
            yield i

    def generate_file_hashes(self, prepared_target):

        self.debug("Hashing file: {}".format(prepared_target))
        for i in self.block_hasher.generate(prepared_target):
            yield i

    def generate_file_compare(self, prepared_target, input_generator=None):

        self.debug("Comparing file: {}".format(prepared_target))
        for i in self.block_hasher.compare(prepared_target, input_generator):
            yield i

    def generate_input(self):
        """parse input lines and yield items to use in compare functions"""

        if self.args.check is True:
            input_fh=sys.stdin
        else:
            input_fh=open(self.args.check, 'r')

        last_progress_time = time.time()
        progress_checked = 0
        progress_skipped = 0

        line=input_fh.readline()
        skip=0
        while line:
            i=line.rstrip().split("\t")
            #ignores lines without tabs
            if (len(i)>1):

                if skip==0:
                    progress_checked=progress_checked+1
                    yield i
                    skip=self.args.skip
                else:
                    skip=skip-1
                    progress_skipped=progress_skipped+1

                if self.args.progress and time.time() - last_progress_time > 1:
                    last_progress_time = time.time()
                    self.progress("Checked {} hashes (skipped {})".format(progress_checked, progress_skipped))

            line=input_fh.readline()

        self.verbose("Checked {} hashes (skipped {})".format(progress_checked, progress_skipped))

    def print_hashes(self, hash_generator):
        """prints hashes that are yielded by the specified hash_generator"""

        last_progress_time = time.time()
        progress_count = 0

        for i in hash_generator:

            if len(i) == 3:
                print("{}\t{}\t{}".format(*i))
            else:
                print("{}\t{}".format(*i))
            progress_count = progress_count + 1

            if self.args.progress and time.time() - last_progress_time > 1:
                last_progress_time = time.time()
                self.progress("Generated {} hashes.".format(progress_count))

            sys.stdout.flush()

        self.verbose("Generated {} hashes.".format(progress_count))
        self.clear_progress()

        return 0

    def print_errors(self, compare_generator):
        """prints errors that are yielded by the specified compare_generator"""
        errors = 0
        for i in compare_generator:
            errors = errors + 1

            if len(i) == 4:
                (file_name, chunk_nr, compare_hexdigest, actual_hexdigest) = i
                print("{}: Chunk {} failed: {} {}".format(file_name, chunk_nr, compare_hexdigest, actual_hexdigest))
            else:
                (chunk_nr, compare_hexdigest, actual_hexdigest) = i
                print("Chunk {} failed: {} {}".format(chunk_nr, compare_hexdigest, actual_hexdigest))

            sys.stdout.flush()

        self.verbose("Total errors: {}".format(errors))
        self.clear_progress()

        return errors

    def prepare_target(self):

        if "@" in self.args.target:
            # zfs snapshot
            snapshot=self.node.get_dataset(self.args.target)
            if not snapshot.exists:
                raise Exception("ZFS snapshot {} does not exist!".format(snapshot))
            dataset_type = snapshot.parent.properties['type']

            if dataset_type == 'volume':
                return self.prepare_zfs_volume(snapshot)
            elif dataset_type == 'filesystem':
                return self.prepare_zfs_filesystem(snapshot)
            else:
                raise Exception("Unknown dataset type")
        return self.args.target

    def cleanup_target(self):
        if "@" in self.args.target:
            # zfs snapshot
            snapshot=self.node.get_dataset(self.args.target)
            if not snapshot.exists:
                return

            dataset_type = snapshot.parent.properties['type']

            if dataset_type == 'volume':
                self.cleanup_zfs_volume(snapshot)
            elif dataset_type == 'filesystem':
                self.cleanup_zfs_filesystem(snapshot)

    def run(self):

        compare_generator=None
        hash_generator=None
        try:
            prepared_target=self.prepare_target()
            is_dir=os.path.isdir(prepared_target)

            #run as compare
            if self.args.check is not None:
                input_generator=self.generate_input()
                if is_dir:
                    compare_generator = self.generate_tree_compare(prepared_target, input_generator)
                else:
                    compare_generator=self.generate_file_compare(prepared_target, input_generator)
                errors=self.print_errors(compare_generator)
            #run as generator
            else:
                if is_dir:
                    hash_generator = self.generate_tree_hashes(prepared_target)
                else:
                    hash_generator=self.generate_file_hashes(prepared_target)

                errors=self.print_hashes(hash_generator)

        except Exception as e:
            self.error("Exception: " + str(e))
            if self.args.debug:
                raise
            return 255
        except KeyboardInterrupt:
            self.error("Aborted")
            return 255

        finally:
            #important to call check_output so that cleanup still functions in case of a broken pipe:
            # util.check_output()

            #close generators, to make sure files are not in use anymore when cleaning up
            if hash_generator is not None:
                hash_generator.close()
            if compare_generator is not None:
                compare_generator.close()
            self.cleanup_target()

        return errors


def cli():
    import sys
    signal(SIGPIPE, sigpipe_handler)
    failed=ZfsCheck(sys.argv[1:], False).run()
    sys.exit(min(failed,255))


if __name__ == "__main__":
    cli()
