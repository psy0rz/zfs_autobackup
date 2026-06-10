# Replicating ZFS clones

(supported in version 4.0 or higher)

A ZFS clone is a writable copy of a snapshot. Clones are space-efficient: they only store the differences with their origin snapshot. They are used a lot by virtualisation platforms for things like linked-clones and templates.

Older versions of zfs-autobackup would replicate each clone as a full standalone dataset, losing the space-efficiency on the target. Since version 4.0, zfs-autobackup preserves the clone relationship during replication.

## How it works

If a selected source dataset is a clone, and its origin dataset is also selected:

* The origin dataset is always replicated before its clones. (zfs-autobackup sorts the datasets to make sure of this)
* The origin snapshot is automatically included in the transfer, even if it doesn't match our `--snapshot-format` and you didn't specify `--other-snapshots`. You will see this in the verbose output: `Including as clone origin for a selected clone`
* The initial transfer of the clone is done as an incremental send from the origin snapshot. This way `zfs recv` reconstructs the clone relationship on the target, and the target stays just as space-efficient as the source.

After the initial transfer, the clone is just synced incrementally like any other dataset.

## When it cant preserve the clone relationship

In some cases zfs-autobackup will warn you and fall back to a full send:

* The origin snapshot is not available on the target. (For example because the origin dataset isn't selected for backup)
* The origin snapshot on the target has a mismatching GUID.
* The clone has a "reverse" topology: after a `zfs promote`, the origin of a dataset can live on one of its own children. ZFS can't receive such a stream, so the clone relationship can't be preserved. (You can `zfs promote` the source dataset to flip the lineage back if you want)

A fallback to full send is never a problem for the consistency of your backup, it just uses more space on the target.

## Disabling clone support

Use `--no-clone` if you want the old behaviour: clones will be replicated as full standalone datasets.
