## Running custom commands before and after snapshotting

You can run commands before and after the snapshot to freeze databases for example, to make the on-disk data consistent before snapshotting.

## When is this needed?

ZFS snapshots are atomic per pool. There wont be any corruption if something tries to write during the making of a snapshot. 

If you restore a snapshot, the application will just think the server (or application) had a regular crash.

Most modern databases and applications can handle this fine, so usually freezing isnt needed in that case. 

Note that if you use multiple pools, the snapshots are only atomic per pool. 
## Method 1: Use snapshot mode

Its possible to use zfs-autobackup in snapshot-only mode. This way you can just create a script that contains the pre and post steps:

```console
#freeze stuff
some-freeze-command

#make snapshot
zfs-autobackup backup1

#unfreeze stuff
some-unfreeze-command

#make backup
zfs-autobackup backup1 --no-snapshot --ssh-target backupserver backups/db1
```

This has the disadvantage that you might have to do the error handling yourself. Also if the source is remote, you have to use the correct ssh command and escaping as well.

## Method 2: Use --pre-snapshot-cmd and --post-snapshot-cmd

With this method, zfs-autobackup will handle the pre and post execution for you.

For example:

```sh
zfs-autobackup \
 --pre-snapshot-cmd 'some-freeze-command'\
 --post-snapshot-cmd 'some-unfreeze-command'\
 --ssh-target backupserver backups/db1
```

The way this works:

* The pre and post commands are always executed on the source side. (via ssh if needed)
* If a pre-command fails, it will immediately execute the post-commands and exit with an error code.
* All post-commands are always executed. Even if the pre-commands or actual snapshot have failed. This way you can be sure that stuff is always cleaned up and unfreezed.
