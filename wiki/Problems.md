# Common problems you might encounter

## It keeps asking for my SSH password

You forgot to setup automatic login via SSH keys, look in the example how to do this.


## "cannot receive incremental stream: destination has been modified since most recent snapshot"

This means your target dataset has been modified somehow.

Make sure you read how [[Mounting]] works.

 * You can use `--rollback` to automatically rollback such changes. 
 * Set the `readonly` property of the target filesystem to on. This prevents all modifications on the target side.  Note that readonly prevents changes to the CONTENTS of the dataset directly. Its still possible to receive new datasets and manipulate properties etc.
 * Always use `--clear-mountpoint`, see [[Mounting]]


## "cannot receive incremental stream: invalid backup stream"

This usually means you've created a new snapshot on the target side during a backup. If you restart zfs-autobackup, it will automaticly abort the invalid partially received snapshot and start over.

## "internal error: Invalid argument"

In some cases (Linux -> FreeBSD) this means certain properties are not fully supported on the target system.

Try using something like: --filter-properties xattr or --ignore-transfer-errors. 

##  zfs receive fails, but snapshot seems to be received successful.

This happens if you transfer between different Operating systems/zfs versions or feature sets.

Try using the --ignore-transfer-errors option. This will ignore the error. It will still check if the snapshot is actually received correctly.

## "cannot receive incremental stream: kernel modules must be upgraded to receive this stream."

This happens if you forget to use --encrypt, while the target datasets are already encrypted. (Very strange error message indeed)

## "dataset is busy"

If you try to destroy a snapshot and its busy, there might be multiple reasons, but dont forget to check if the snapshot has holds with:

> zfs holds `zpool/fs1@snapshot1`

Release it with zfs release.

Use --no-holds in zfs-autobackup if you dont want to use holds.

Note: Since version 4.0 this problem mostly went away on the source side, because zfs-autobackup uses bookmarks there instead of holds. See [[Common-snapshots-and-holds]]

## "--exclude-received is no longer needed"

The `--exclude-received` option is removed in version 4.0: zfs-autobackup now always filters all the `autobackup:...` properties for newly received datasets, which solves the recursive replication problem in a better way.

Remove the option from your scripts. To get rid of existing properties on the target do this one time:

> zfs inherit -r autobackup:backup1 pool/backup1

## Other problems

 * Feel free to ask questions about zfs-autobackup here: https://github.com/psy0rz/zfs_autobackup/discussions

 * If you found a bug or want to request a feature: https://github.com/psy0rz/zfs_autobackup/issues

 * For more general questions go to the excellent subreddit at https://reddit.com/r/zfs
