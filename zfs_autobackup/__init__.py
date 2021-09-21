


def cli():
    import sys
    from .ZfsAutobackup import ZfsAutobackup

    zfs_autobackup = ZfsAutobackup(sys.argv[1:], False)
    sys.exit(zfs_autobackup.run())
