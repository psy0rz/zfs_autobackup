import hashlib

from .util import block_hash
from .CliBase import CliBase




class ZfsCheck(CliBase):

    def __init__(self, argv, print_arguments=True):

        # NOTE: common options and parameters are in ZfsAuto
        super(ZfsCheck, self).__init__(argv, print_arguments)

    def run(self):



        # print(sha1sum("/home/psy/Downloads/carimage.zip"))
        for (block, h ) in block_hash("/home/psy/Downloads/carimage.zip" , count=10000):
            print(block)
            print (h)


        pass

def cli():
    import sys

    sys.exit(ZfsCheck(sys.argv[1:], False).run())

if __name__ == "__main__":
    cli()
