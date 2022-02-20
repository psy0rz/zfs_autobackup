import hashlib

from .ZfsNode import ZfsNode
from .util import *
from .CliBase import CliBase

class ZfsCheck(CliBase):

    def __init__(self, argv, print_arguments=True):

        # NOTE: common options and parameters are in ZfsAuto
        super(ZfsCheck, self).__init__(argv, print_arguments)

        self.node=ZfsNode(self.log, readonly=self.args.test, debug_output=self.args.debug_output)


    def get_parser(self):

        parser=super(ZfsCheck, self).get_parser()

        #positional arguments
        parser.add_argument('snapshot', metavar='SNAPSHOT', default=None, nargs='?',
                            help='Snapshot to checksum')


        group=parser.add_argument_group('Hasher options')

        group.add_argument('--block-size', metavar="BYTES", default=4096, help="Read block-size, default %(default)s", type=int)
        group.add_argument('--count', metavar="COUNT", default=int((100*(1024**2))/4096), help="Generate a hash for every COUNT blocks. default %(default)s", type=int) #100MiB

        return parser

    def parse_args(self, argv):
        args=super(ZfsCheck, self).parse_args(argv)

        if args.test:
            self.warning("TEST MODE - NOT DOING ANYTHING USEFULL")
            self.log.show_debug=True #show at least what we would do

        return args

    def hash_filesystem(self, snapshot):
        """

        :type snapshot: ZfsDataset.ZfsDataset
        """
        mnt="/tmp/"+tmp_name()

        try:
            self.debug("Create temporary mount point {}".format(mnt))
            self.node.run(["mkdir", mnt])

            snapshot.mount(mnt)

            self.debug("Hashing tree: {}".format(mnt))
            if not self.args.test:
                for (file, block, hash) in block_hash_tree(mnt):
                    print("{}\t{}\t{}".format(file, block, hash))

        finally:
            self.debug("Cleaning up temporary mount point")
            snapshot.unmount()
            self.node.run(["rmdir", mnt], hide_errors=True, valid_exitcodes=[])


    def run(self):

        snapshot=self.node.get_dataset(self.args.snapshot)

        if not snapshot.exists:
            snapshot.error("Dataset not found")
            sys.exit(1)

        if not snapshot.is_snapshot:
            snapshot.error("Dataset should be a snapshot")
            sys.exit(1)

        dataset_type=snapshot.parent.properties['type']

        if dataset_type=='volume':
            self.checksum_volume(snapshot)
        elif dataset_type=='filesystem':
            self.hash_filesystem(snapshot)
        else:
            raise Exception("huh?")

        pass

def cli():
    import sys

    sys.exit(ZfsCheck(sys.argv[1:], False).run())

if __name__ == "__main__":
    cli()
