


def cli():
    import sys
    from .ZfsAutobackup import ZfsAutobackup

    zfs_autobackup = ZfsAutobackup(sys.argv[1:], False)
    failed_datasets=zfs_autobackup.run()
    sys.exit(min(failed_datasets,255))
