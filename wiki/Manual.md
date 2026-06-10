
## Reference manual

If you're new to zfs-autobackup you should read the [Getting started](Home) page first. It shows you how to get things done.

Then read this full manual to understand how zfs-autobackup works, and what all the options are doing. 

Both guides are complementary.

### Usage

```
usage: ZfsAutobackup.py [--help] [--test] [--verbose] [--debug]
                        [--debug-output] [--progress] [--utc] [--version]
                        [--ssh-config CONFIG-FILE] [--ssh-source USER@HOST]
                        [--ssh-target USER@HOST] [--property-format FORMAT]
                        [--snapshot-format FORMAT] [--hold-format FORMAT]
                        [--strip-path N] [--tag-seperator STRING] [--tag TAG]
                        [--exclude-unchanged BYTES]
                        [--exclude-snapshot-pattern EXCLUDE_SNAPSHOT_PATTERN]
                        [--no-snapshot] [--pre-snapshot-cmd COMMAND]
                        [--post-snapshot-cmd COMMAND] [--min-change BYTES]
                        [--allow-empty] [--other-snapshots]
                        [--set-snapshot-properties PROPERTY=VALUE,...]
                        [--no-guid-check] [--no-send] [--no-holds]
                        [--no-bookmarks] [--no-clone] [--clear-refreservation]
                        [--clear-mountpoint]
                        [--filter-properties PROPERTY,...]
                        [--set-properties PROPERTY=VALUE,...] [--rollback]
                        [--force] [--destroy-incompatible]
                        [--ignore-transfer-errors] [--decrypt] [--encrypt]
                        [--zfs-compressed] [--compress [TYPE]]
                        [--rate DATARATE] [--buffer SIZE]
                        [--buffer-chunk-size BUFFERCHUNKSIZE]
                        [--send-pipe COMMAND] [--recv-pipe COMMAND]
                        [--no-thinning] [--keep-source SCHEDULE]
                        [--keep-target SCHEDULE] [--destroy-missing SCHEDULE]
                        [BACKUP-NAME] [TARGET-PATH]

ZfsAutobackup.py v4.0-beta3 - (c)2026 E.H.Eefting (edwin@datux.nl)

positional arguments:
  BACKUP-NAME           Name of the backup to select
  TARGET-PATH           Target ZFS filesystem (optional)

Common options:
  --help, -h            show help
  --test, --dry-run, -n
                        Dry run, dont change anything, just show what would be
                        done (still does all read-only operations)
  --verbose, -v         verbose output
  --debug, -d           Show zfs commands that are executed, stops after an
                        exception.
  --debug-output        Show zfs commands and their output/exit codes. (noisy)
  --progress            show zfs progress output. Enabled automaticly on ttys.
                        (use --no-progress to disable)
  --utc                 Use UTC instead of local time when dealing with
                        timestamps for both formatting and parsing. To
                        snapshot in an ISO 8601 compliant time format you may
                        for example specify --snapshot-format
                        "{}-%Y-%m-%dT%H:%M:%SZ". Changing this parameter
                        after-the-fact (existing snapshots) will cause their
                        timestamps to be interpreted as a different time than
                        before.
  --version             Show version.

SSH options:
  --ssh-config CONFIG-FILE
                        Custom ssh client config
  --ssh-source USER@HOST
                        Source host to pull backup from.
  --ssh-target USER@HOST
                        Target host to push backup to.

String formatting options:
  --property-format FORMAT
                        Name of the ZFS user-property used to select datasets
                        for this backup. The literal "{}" is substituted with
                        BACKUP-NAME. Default: autobackup:{}
  --snapshot-format FORMAT
                        Format of the snapshot-name part after the "@". The
                        literal "{}" is substituted with BACKUP-NAME, then
                        strftime-codes (e.g. %Y %m %d %H %M %S) are expanded
                        against the current time. Must not contain the tag-
                        seperator. Default: {}-%Y%m%d%H%M%S
  --hold-format FORMAT  Name of the zfs-hold placed on snapshots to prevent
                        accidental deletion. The literal "{}" is substituted
                        with BACKUP-NAME. Default: zfs_autobackup:{}
  --strip-path N        Number of directories to strip from target path.
  --tag-seperator STRING
                        Tag seperator for snapshots and bookmarks. Default: __
  --tag TAG             Backup tag to add to snapshots names. (For
                        administrative purposes)

Selection options:
  --exclude-unchanged BYTES
                        Exclude datasets that have less than BYTES data
                        changed since any last snapshot. (Use with proxmox HA
                        replication)
  --exclude-snapshot-pattern EXCLUDE_SNAPSHOT_PATTERN
                        Regular expression to match snapshots that will be
                        ignored.

Snapshot options:
  --no-snapshot         Don't create new snapshots (useful for finishing
                        uncompleted backups, or cleanups)
  --pre-snapshot-cmd COMMAND
                        Run COMMAND before snapshotting (can be used multiple
                        times.
  --post-snapshot-cmd COMMAND
                        Run COMMAND after snapshotting (can be used multiple
                        times.
  --min-change BYTES    Only create snapshot if enough bytes are changed.
                        (default 1)
  --allow-empty         If nothing has changed, still create empty snapshots.
                        (Same as --min-change=0)
  --other-snapshots     Send over other snapshots as well, not just the ones
                        created by this tool.
  --set-snapshot-properties PROPERTY=VALUE,...
                        List of properties to set on the snapshot.
  --no-guid-check       Dont check guid of common snapshots. (faster)

Transfer options:
  --no-send             Don't transfer snapshots (useful for cleanups, or if
                        you want a separate send-cronjob)
  --no-holds            Don't hold snapshots. (Faster. Allows you to destroy
                        common snapshot.)
  --no-bookmarks        Don't use bookmarks.
  --no-clone            Don't preserve clone relationships during replication.
                        Source datasets that are clones will be replicated as
                        full standalone datasets.
  --clear-refreservation
                        Filter "refreservation" property. (recommended, saves
                        space. same as --filter-properties refreservation)
  --clear-mountpoint    Set property canmount=noauto for new datasets.
                        (recommended, prevents mount conflicts. same as --set-
                        properties canmount=noauto)
  --filter-properties PROPERTY,...
                        List of properties to "filter" when receiving
                        filesystems. (you can still restore them with zfs
                        inherit -S)
  --set-properties PROPERTY=VALUE,...
                        List of propererties to override when receiving
                        filesystems. (you can still restore them with zfs
                        inherit -S)
  --rollback            Rollback changes to the latest target snapshot before
                        starting. (normally you can prevent changes by setting
                        the readonly property on the target_path to on)
  --force, -F           Use zfs -F option to force overwrite/rollback. (Useful
                        with --strip-path=1, but use with care)
  --destroy-incompatible
                        Destroy incompatible snapshots on target. Use with
                        care! (also does rollback of dataset)
  --ignore-transfer-errors
                        Ignore transfer errors (still checks if received
                        filesystem exists. useful for acltype errors)
  --decrypt             Decrypt data before sending it over.
  --encrypt             Encrypt data after receiving it.
  --zfs-compressed      Transfer blocks that already have zfs-compression as-
                        is.

Data transfer options:
  --compress [TYPE]     Use compression during transfer, defaults to zstd-fast
                        if TYPE is not specified. (gzip, pigz-fast, pigz-slow,
                        zstd-fast, zstd-slow, zstd-adapt, xz, lzo, lz4)
  --rate DATARATE       Limit data transfer rate in Bytes/sec (e.g. 128K.
                        requires mbuffer.)
  --buffer SIZE         Add zfs send and recv buffers to smooth out IO bursts.
                        (e.g. 128M. requires mbuffer)
  --buffer-chunk-size BUFFERCHUNKSIZE
                        Tune chunk size when mbuffer is used. (requires
                        mbuffer.)
  --send-pipe COMMAND   pipe zfs send output through COMMAND (can be used
                        multiple times)
  --recv-pipe COMMAND   pipe zfs recv input through COMMAND (can be used
                        multiple times)

Thinner options:
  --no-thinning         Do not destroy any snapshots.
  --keep-source SCHEDULE
                        Thinning schedule for old source snapshots. Default:
                        10,1d1w,1w1m,1m1y
  --keep-target SCHEDULE
                        Thinning schedule for old target snapshots. Default:
                        10,1d1w,1w1m,1m1y
  --destroy-missing SCHEDULE
                        Destroy datasets on target that are missing on the
                        source. Specify the time since the last snapshot, e.g:
                        --destroy-missing 30d

Full manual at: https://github.com/psy0rz/zfs_autobackup
```

## Safe defaults

zfs-autobackup uses safe defaults such as:

* Preserving all dataset properties.  This can have its drawbacks to, for example [Mounting](Mounting)
* Preserving full dataset paths. Look [here](Home#stripping-the-path) how to change that.
* Only modify snapshots that match the zfs-autobackup format.
* Not rolling back or forcing anything.
* Checking everything, failing early and in a verbose manner.
* Don't do anything unexpected.

Keeping this in mind can help make more sense of the options described here: most of the options are to modify these safe defaults.

### Good for scripting: No config files 

zfs-autobackup has no config files on purpuse, this makes it very suitable to use in scripts if you have more complex setups.

For example, see: https://github.com/psy0rz/zfs_autobackup/wiki/Example-Proxmox

So keep this in mind in case you're wondering why zfs-autobackup does it like this. 

## Common options

You probably already know these from the Getting started guide, scroll on to Step 1 in that case.

### Testing and debugging

It's recommended to always use `--verbose` or `-v` to see whats going on. It makes debugging easier.

During initial setup and testing of a backup you should use `--test`. This will perform all the read-only operations, but will not change anything. It will show you exactly what it's going to do.

If you encounter a problem and want to see the exact ZFS commands, use `--debug`. This outputs all the underlying zfs-commands in a different color. To see the output of each command, use `--debug-output`.

Note that debug mode will abort on the first failed dataset, and show a stacktrace so it's not recommended for use in production.

### SSH source and target options

zfs-autobackup can be used locally or remotely via ssh.

These options are for backing up to or from remote hosts via ssh:

  * `--ssh-source USER@HOST`: Source host to pull backup from.
  * `--ssh-target USER@HOST`: Target host to push backup to.

If you dont specify a source or target host, zfs-autobackup will operate locally.

Things like different ssh-ports should be configured in your `~/.ssh/config file`. (Or the one specified with `--ssh-config CONFIG-FILE`)

# Backup procedure

zfs-autobackup always performs its operations in a certain order.

All these steps are described here:

## Step 1: Selecting

This step selects the datasets that are part of the run.

### Dataset property

Selection is done by a dataset property. The name of this property is the `backup-name`, formatted by `--property-format`. The default is `autobackup:backup-name`.

The zfs-autobackup property can have the following values:

* `true`: Select the dataset and all its children.
* `false`: Exclude the dataset and all its children.
* `child`: Only select the children of the dataset, not the dataset itself.
* `parent`: Only select the parent, but not the children. (supported in version 3.2 or higher)

If there are no datasets that have this property set then zfs-autobackup exits with an error.

### Further exclusions

Datasets can also be excluded from selection by these options:

* `--exclude-unchanged BYTES`: Exclude datasets that have less than BYTES data changed since the last snapshot. (Use with proxmox HA replication)

NOTE: The `--exclude-received` option is removed in version 4.0: zfs-autobackup now always filters all `autobackup:...` properties when receiving datasets, so recursive replication between backup partners can't happen anymore. To get rid of existing properties on the target do this one time: `zfs inherit -r autobackup:backup1 pool/backup1`

## Step 2: Snapshotting

In this step a snapshot is created on the datasets selected in step 1.

zfs-autobackup creates atomic snapshots per pool. This is a single `zfs snapshot` command that includes all the snapshots that need to be taken for that pool.

Snapshotting can be skipped with `--no-snapshot`. Using this option will result in only syncing existing snapshots.

### Snapshot format

Snapshots are created using a specific naming format. This includes a timestamp that zfs-autobackup uses to determine when a snapshot can be destroyed by the [Thinner](Thinner).

It is possible to change this format by using `--snapshot-format`. Other snapshots that do not match this format are normally ignored by zfs-autobackup. Use `--utc` to use UTC for timestamps.

We use python datetime formatting, a table is on the end at this page: https://www.w3schools.com/python/python_datetime.asp

### Tags

(supported in version 4.0 or higher)

With `--tag` you can add an administrative tag to the snapshot names. For example `--tag offsite1` results in snapshot names like `backup1-20260610120000__offsite1`.

Tags are purely for your own administrative purposes: zfs-autobackup ignores the tag-part when matching and thinning snapshots. This way you can see which job created a snapshot, while jobs with different tags still use each others snapshots as common snapshot.

The tag is separated from the timestamp by the `--tag-seperator`, which defaults to `__`. (According to `man 8 zfs` the allowed characters are `_.: -`)

You can also ignore certain snapshots completely with `--exclude-snapshot-pattern`, which takes a regular expression. Excluded snapshots will not be transferred or destroyed.

### Pre- and post snapshot commands

You can run commands pre- and post-snapshotting with `--pre-snapshot-cmd` and `--post-snapshot-cmd.`

More info [here](PrePost).


### Skipping conditions

Snapshot creation will be skipped for datasets that have no changes since the last snapshot.

This can be controlled by:
* `--min-change BYTES`: Only create snapshot if enough bytes have changed. (default 1)
* `--allow-empty`: If nothing has changed, still create empty snapshots. (Same as --min-change=0)

### Other options

* `--set-snapshot-properties PROPERTY=VALUE,...`: List of properties to set on the new snapshot.

## Step 3: Synchronising

Syncronisation is done only if `TARGET-PATH` is specified. Otherwise zfs-autobackup is just a snapshot tool and stops after step 2.

For each selected source dataset it does the following steps:

### Step 3.1: Planning

If the target dataset already exists:
* We determine the [Common snapshot or bookmark](Common-snapshots-and-holds). Bookmarks are preferred over snapshots. If name-matching fails, we also do an extensive GUID-search, so we can even sync from snapshots or bookmarks that were created by other tools.
* We check the GUID of the common snaphot, unless `--no-guid-check` is set.
* We determine a list of incompatible snapshots that are in the way. (after our common snapshot)
* If there isn't a valid common snapshot or bookmark, the dataset fails with instructions on how to continue. (Use `-F` to overwrite the target dataset, or `--destroy-incompatible -F` if it also has snapshots)

If the source dataset is a ZFS clone, we will try to preserve the clone relationship on the target. See [Clones](Clones).

We determine which snapshots are kept and which ones can be destroyed by the [Thinner](Thinner). Note that only our own snapshots (matching the `--snapshot-format`), are considered for deletion.

If `--no-thinning` is used, this list of obsolete snapshots will always be empty.

### Step 3.2: Pre-clean

After planning, provided `--no-thinning` isn't used, we destroy obsolete snapshots on the source and target to save space during sync.

### Step 3.3: Destroy incompatible snapshots

If the planner has detected incompatible snapshots, we will destroy them. But since this can be dangerous and is normally not needed, you have to enable this with `--destroy-incompatible`

Otherwise the dataset will fail.

### Step 3.4: Transferring snapshots

Now the snapshots are actually transferred, unless `--no-send` is used.

If `--other-snapshots` is specified, we will also transfer snapshots that do not match our `--snapshot-format`. These other snapshots will never be destroyed.

For each snapshot we:
* Check if we need to resume an aborted transfer.
* Handle [Encryption](Encryption) options. (`--encrypt` and `--decrypt`)
* Transfer the data, using various [Transfer options](Piping) (`--zfs-compressed`, `--compress`, `--send-pipe`, `--recv-pipe`, `--buffer`, `--rate`)
* Filter/set properties according to `--set-properties` and `--filter-properties`. Properties starting with `autobackup:` are always filtered.
* Create a [bookmark](Common-snapshots-and-holds) of the new common snapshot on the source, and hold the common snapshot on the target. Use `--no-bookmarks` to use holds on the source instead, like older versions did. (Use `--hold-format` to specify the name of the hold, and `--no-holds` to disable holds completely)

Just before the first snapshot is transferred, we do a rollback, if `--rollback` is specified.

If it's an initial transfer that created a new target dataset, we try to [automount](Mounting) the target after the first snapshot is transferred.

We destroy obsolete snapshots from the planning phase as soon as possible. (`--no-thinning` effectively disables this )

#### Extra transfer options


* `--ignore-transfer-errors`: Ignore ZFS transfer errors. It still checks if received filesystem exists. This is useful to ignore some acltype errors.
* `--clear-refreservation`: Filter "refreservation" property. Recommended to save space. Same as `--filter-properties` refreservation.
* `--clear-mountpoint`:  Set property `canmount=noauto` for new datasets.  Recommended, prevents mount conflicts. Same as `--set-properties canmount=noauto`. Also see [Mounting](Mounting)
* `--strip-path N`: Number of directories to strip from target path. An example is given in the Getting started guide.
* `--force`: Use `zfs -F` option to force overwrite/rollback. Useful with `--strip-path=1`. Use with care!

## Step 4: Handle missing datasets

Datasets that are missing or deselected, but still exist in the target-path are called missing datasets.

The handling of those is described [here](Thinner#destroying-missing-datasets) (`--destroy-missing` )


## Thinner

The thinner decides when a snapshot is obsolete. Look at [Thinner](Thinner) for more info. (`--keep-source` and `--keep-target`)


## Running without root

In order to run zfs-autobackup without root permissions, you'll need to set a few ZFS permissions. The permissions required differ for receiving and sending.

On the machine you want to sync the dataset from, you'll need these permissions:

```console
root@source:~# zfs allow -u localuser mount,send,hold,snapshot,destroy,release,bookmark rpool
```

(The `bookmark` permission is needed since version 4.0, unless you use `--no-bookmarks`)

On the receiving side, you need these:

```console
root@target:~# zfs allow -u remoteuser compression,mountpoint,create,mount,receive,rollback,destroy,release,hold,userprop,readonly,canmount,dedup tank/backups/rpool
```

Depending on your use-case, you can always try to restrict these permissions further.

### /dev/zfs permissions

On some distributions like Alpine linux, you will need to change the permissions of /dev/zfs, so that regular users can write to it. This is safe, since the zfs kernel module handles access restrictions internally. (via zfs allow etc)

If you have trouble, make sure to check this, or just fix it with `chmod 666 /dev/zfs`


