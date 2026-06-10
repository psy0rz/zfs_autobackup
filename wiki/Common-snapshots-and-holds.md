## Common snapshots, bookmarks and holds

If you're new to ZFS these terms can be quite confusing. Whats going on?

**TLDR:** Since version 4.0 zfs-autobackup uses bookmarks on the source side by default. You usually dont have to do or worry about anything.

## Common snapshots

ZFS can do incremental transfers via snapshots. It does this very efficiently by sending over the differences between two snapshots.

There are a few rules for ZFS however:

* The same starting snapshot has to exist on both target and source. So its a common snapshot. (On the source side a bookmark of that snapshot is enough, see below)
* There cant be any newer snapshots on the target. (Normally should not happen, otherwise use --destroy-incompatible)
* Encryption has to be compatible (See [[Encryption]])

**If there is no common snapshot or bookmark, the only way to continue is to overwrite the target dataset by running zfs-autobackup with -F.** This resends the whole dataset. (If the target dataset also has snapshots in the way, zfs-autobackup will tell you to use `--destroy-incompatible -F`)

Since version 4.0 zfs-autobackup can also find the common snapshot via an extensive GUID-search: even if the snapshot or bookmark names dont match (because they were created by another tool for example), it can still find and use them. This makes migrating from other tools much easier.

## Bookmarks (version 4.0 and higher)

A ZFS bookmark is a tiny marker that remembers a point-in-time of a dataset. You can use it as the starting point of an incremental `zfs send`, just like a snapshot. The big difference: a bookmark takes up almost no space and doesn't keep any data alive.

After a snapshot is transferred to the target, zfs-autobackup creates a bookmark of it on the source. From that point on the actual source snapshot is allowed to be destroyed: the bookmark is enough to send the next increment.

Advantages over the old holds-method:

* The common snapshot on the source can be destroyed by the [Thinner](Thinner). With `--keep-source=0` almost nothing is kept on the source, saving a lot of space.
* No more "dataset is busy" frustrations on the source side.
* You can still safely destroy source snapshots manually.

You can see the bookmarks with `zfs list -t bookmark`. They look like:

```
rpool/data#offsite1-20260610120000__12345678901234567890
```

The number after the `__` tag-seperator is the GUID of the target dataset: This way you can send the same backup to multiple targets, and each target gets its own bookmark without interfering with the others.

If the source or target pool doesn't support bookmarks, zfs-autobackup will warn you and automatically fall back to using holds.

Use `--no-bookmarks` if you want the old behaviour with holds on the source. (You can switch back and forth without problems)

## Holds

To prevent accidental deletion of the common snapshot on the **target**, we use "holds". A snapshot that is held cannot be destroyed, until its released with `zfs release`. (Use `zfs holds` to see the holds for a specific snapshot)

zfs-autobackup will automatically hold the common snapshot on the target. It will automatically release it as soon as there is a newer common snapshot.

Before version 4.0 the source side also used holds instead of bookmarks. This could be quite frustrating for new users who tried to delete old datasets that still had holds. (`Dataset is busy`)

Use `--no-holds` if you dont want any holds. In that case its up to you to make sure the common snapshot isn't destroyed.

## Holds, bookmarks and offline backups

Normally when you split up the snapshotting part and backupping part you would do it like this: [[https://github.com/psy0rz/zfs_autobackup/wiki#splitting-up-snapshot-and-backup-job]]

The snapshotter will still connect to the target server and figure out the common snapshot so that they wont be destroyed. It can also cleanup old snapshots from the source if it sees that target doesn't need them (anymore)

However, if you have an offline backup (e.g. a USB disk that you sometimes connect), you are forced to use the snapshot-only tool. You would just run zfs-autobackup without specifying a target dataset or ssh-target. In that case it only makes snapshots and cleans up old snapshots according to the --keep-source schedule.

Since version 4.0 this is much less of a problem: the bookmark of the common snapshot is never destroyed by the thinner, so even with a tight --keep-source schedule the incremental transfer will still work the next time you connect your offline backup.

If you use `--no-bookmarks`, the holds become very important again: in snapshot-only mode zfs-autobackup looks at the holds to see which snapshots are common. Otherwise it might accidentally destroy them if you have a tight --keep-source schedule. So in that case: never use --no-holds.
