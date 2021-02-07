
#python 2 compatibility
from __future__ import print_function


def cli():
    import sys
    from zfs_autobackup.ZfsAutobackup import ZfsAutobackup

    zfs_autobackup = ZfsAutobackup(sys.argv[1:], False)
    sys.exit(zfs_autobackup.run())
