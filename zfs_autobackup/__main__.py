# (c)edwin@datux.nl  - Released under GPL V3
#
# Greetings from eth0 2019 :)

import sys

if __name__ == "__main__":
    from zfs_autobackup.ZfsAutobackup import ZfsAutobackup
    zfs_autobackup = ZfsAutobackup(sys.argv[1:], False)
    sys.exit(zfs_autobackup.run())

