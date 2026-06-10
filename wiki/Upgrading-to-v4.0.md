# Upgrading to version 4.0

Version 4.0 changes some things in how zfs-autobackup operates by default. This page lists what changed and what you might have to do.

**TLDR:** Upgrading is safe: existing backups just keep working and you usually dont have to change anything. The only hard change is `--exclude-received`: remove it from your scripts.

## Bookmarks instead of holds on the source

After a successful transfer, the source now creates a ZFS bookmark of the common snapshot, instead of holding the snapshot. This means the source snapshot itself can be destroyed by the [Thinner](Thinner), saving space. With `--keep-source=0` almost nothing is kept on the source anymore.

* Existing backups will automatically start using bookmarks on the next run.
* If a pool doesn't support bookmarks, zfs-autobackup warns you and falls back to holds.
* Use `--no-bookmarks` to get the old behaviour.
* If you run without root, the user needs the `bookmark` zfs-permission on the source. See [Running without root](Manual#running-without-root)

More info: [[Common-snapshots-and-holds]]

## --exclude-received is removed

zfs-autobackup now always filters **all** `autobackup:...` properties when receiving datasets. This solves the recursive replication problem that `--exclude-received` was for, so the option is removed. Using it results in an error.

* Remove `--exclude-received` from your scripts.
* To get rid of existing autobackup-properties on the target, do this one time: `zfs inherit -r autobackup:backup1 pool/backup1`

## Clone support

Source datasets that are ZFS clones now keep their clone relationship on the target, so the target stays just as space-efficient as the source. Use `--no-clone` for the old behaviour. More info: [[Clones]]

## Better handling of missing common snapshots

If there is no common snapshot or bookmark, you no longer have to destroy the target dataset and its children manually. zfs-autobackup now tells you exactly what to do: use `-F` to overwrite the target dataset, or `--destroy-incompatible -F` if it also has snapshots in the way.

Also, zfs-autobackup can now find the common snapshot via a GUID-search, even if the snapshot or bookmark names dont match. This makes migrating from other snapshot tools much easier.

## Snapshot tags

You can add an administrative tag to snapshot names with `--tag`. Tags are ignored when matching and thinning snapshots. See [Tags](Manual#tags)

If your snapshot-format or existing snapshot names contain the default tag-seperator `__`, you might have to change the seperator with `--tag-seperator`.

## Other changes

* `--exclude-snapshot-pattern`: ignore snapshots matching a regular expression.
* `--buffer-chunk-size`: tune the mbuffer chunk size, also see [[Piping]].
* `--keep-source=0` now automatically implies `--min-change=0`, which is a significant speedup if you have lots of datasets.
* Better progress output: you now see counters like `[5/100]` and the number of failed datasets while it runs.
* `--decrypt` now warns that properties will not be sent over for decrypted datasets, due to a ZFS bug. See [[Encryption]]
* Python 2 support is dropped, the minimum python version is now 3.2.
