# ZFS autobackup

## New in v3

* Complete rewrite, cleaner object oriented code.
* Python 3 and 2 support.
* Installable via pip.
* Backwards compatible with your current backups and parameters.
* Progressive thinning (via a destroy schedule. default schedule should be fine for most people)
* Cleaner output, with optional color support (pip install colorama).
* Clear distinction between local and remote output.
* Summary at the beginning, displaying what will happen and the current thinning-schedule.
* More effient destroying/skipping snaphots on the fly. (no more space issues if your backup is way behind)
* Progress indicator (--progress)
* Better property management (--set-properties and --filter-properties)
* Better resume handling, automaticly abort invalid resumes.
* More robust error handling.
* Prepared for future enhanchements.
* Supports raw backups for encryption.

## Introduction

ZFS autobackup is used to periodicly backup ZFS filesystems to other locations. This is done using the very effcient zfs send and receive commands.

It has the following features:

* Works across operating systems: Tested with Linux, FreeBSD/FreeNAS and SmartOS.
* Works in combination with existing replication systems. (Like Proxmox HA)
* Automatically selects filesystems to backup by looking at a simple ZFS property. (recursive)
* Creates consistent snapshots. (takes all snapshots at once, atomic.)
* Multiple backups modes:
  * Backup local data on the same server.
  * "push" local data to a backup-server via SSH.
  * "pull" remote data from a server via SSH and backup it locally.
  * Or even pull data from a server while pushing the backup to another server.
* Can be scheduled via a simple cronjob or run directly from commandline.
* Supports resuming of interrupted transfers. (via the zfs extensible_dataset feature)
* Backups and snapshots can be named to prevent conflicts. (multiple backups from and to the same filesystems are no problem)
* Always creates a new snapshot before starting.
* Checks everything but tries continue on non-fatal errors when possible. (Reports error-count when done)
* Ability to 'finish' aborted backups to see what goes wrong.
* Easy to debug and has a test-mode. Actual unix commands are printed.
* Keeps latest X snapshots remote and locally. (default 30, configurable)
* Uses zfs-holds on important snapshots so they cant be accidentally destroyed.
* Easy installation:
  * Just install zfs_autobackup via pip, or download it manually.
  * Written in python and uses zfs-commands, no 3rd party dependency's or libraries.
  * No separate config files or properties. Just one zfs_autobackup-command you can copy/paste in your backup script.

## Installation

Use pip or easy_install to install:

```console
[root@server ~]# pip install zfs_autobackup

```

Its also possible to just download <https://raw.githubusercontent.com/psy0rz/zfs_autobackup/v3/bin/zfs_autobackup> and run it directly.

## Usage

```console
[root@server ~]# zfs_autobackup --help
usage: zfs_autobackup [-h] [--ssh-source SSH_SOURCE] [--ssh-target SSH_TARGET]
                      [--keep-source KEEP_SOURCE] [--keep-target KEEP_TARGET]
                      [--no-snapshot] [--allow-empty] [--ignore-replicated]
                      [--no-holds] [--resume] [--strip-path STRIP_PATH]
                      [--buffer BUFFER] [--clear-refreservation]
                      [--clear-mountpoint]
                      [--filter-properties FILTER_PROPERTIES]
                      [--set-properties SET_PROPERTIES] [--rollback]
                      [--ignore-transfer-errors] [--raw] [--test] [--verbose]
                      [--debug] [--debug-output] [--progress]
                      backup_name target_path

ZFS autobackup 3.0-beta6

positional arguments:
  backup_name           Name of the backup (you should set the zfs property
                        "autobackup:backup-name" to true on filesystems you
                        want to backup
  target_path           Target ZFS filesystem

optional arguments:
  -h, --help            show this help message and exit
  --ssh-source SSH_SOURCE
                        Source host to get backup from. (user@hostname)
                        Default None.
  --ssh-target SSH_TARGET
                        Target host to push backup to. (user@hostname) Default
                        None.
  --keep-source KEEP_SOURCE
                        Thinning schedule for old source snapshots. Default:
                        10,1d1w,1w1m,1m1y
  --keep-target KEEP_TARGET
                        Thinning schedule for old target snapshots. Default:
                        10,1d1w,1w1m,1m1y
  --no-snapshot         dont create new snapshot (usefull for finishing
                        uncompleted backups, or cleanups)
  --allow-empty         if nothing has changed, still create empty snapshots.
  --ignore-replicated   Ignore datasets that seem to be replicated some other
                        way. (No changes since lastest snapshot. Usefull for
                        proxmox HA replication)
  --no-holds            Dont lock snapshots on the source. (Usefull to allow
                        proxmox HA replication to switches nodes)
  --resume              support resuming of interrupted transfers by using the
                        zfs extensible_dataset feature (both zpools should
                        have it enabled) Disadvantage is that you need to use
                        zfs recv -A if another snapshot is created on the
                        target during a receive. Otherwise it will keep
                        failing.
  --strip-path STRIP_PATH
                        number of directory to strip from path (use 1 when
                        cloning zones between 2 SmartOS machines)
  --buffer BUFFER       Use mbuffer with specified size to speedup zfs
                        transfer. (e.g. --buffer 1G) Will also show nice
                        progress output.
  --clear-refreservation
                        Filter "refreservation" property. (recommended, safes
                        space. same as --filter-properties refreservation)
  --clear-mountpoint    Filter "canmount" property. You still have to set
                        canmount=noauto on the backup server. (recommended,
                        prevents mount conflicts. same as --filter-properties
                        canmount)
  --filter-properties FILTER_PROPERTIES
                        List of propererties to "filter" when receiving
                        filesystems. (you can still restore them with zfs
                        inherit -S)
  --set-properties SET_PROPERTIES
                        List of propererties to override when receiving
                        filesystems. (you can still restore them with zfs
                        inherit -S)
  --rollback            Rollback changes on the target before starting a
                        backup. (normally you can prevent changes by setting
                        the readonly property on the target_path to on)
  --ignore-transfer-errors
                        Ignore transfer errors (still checks if received
                        filesystem exists. usefull for acltype errors)
  --raw                 For encrypted datasets, send data exactly as it exists
                        on disk.
  --test                dont change anything, just show what would be done
                        (still does all read-only operations)
  --verbose             verbose output
  --debug               Show zfs commands that are executed, stops after an
                        exception.
  --debug-output        Show zfs commands and their output/exit codes. (noisy)
  --progress            show zfs progress output (to stderr)

When a filesystem fails, zfs_backup will continue and report the number of
failures at that end. Also the exit code will indicate the number of failures.

```

## Backup example

In this example we're going to backup a machine called `pve` to our backupserver.

Its important to choose a unique and consistent backup name. In this case we name our backup: `offsite1`.

### Select filesystems to backup

On the source zfs system set the ```autobackup:offsite``` zfs property to true:

```console
[root@pve ~]# zfs set autobackup:offsite1=true rpool
[root@pve ~]# zfs get -t filesystem,volume autobackup:offsite1
NAME                                    PROPERTY             VALUE                SOURCE
rpool                                   autobackup:offsite1  true                 local
rpool/ROOT                              autobackup:offsite1  true                 inherited from rpool
rpool/ROOT/pve-1                        autobackup:offsite1  true                 inherited from rpool
rpool/data                              autobackup:offsite1  true                 inherited from rpool
rpool/data/vm-100-disk-0                autobackup:offsite1  true                 inherited from rpool
rpool/swap                              autobackup:offsite1  true                 inherited from rpool
...
```

Because we dont want to backup everything, we can exclude certain filesystem by setting the property to false:

```console
[root@pve ~]# zfs set autobackup:offsite1=false rpool/swap
[root@pve ~]# zfs get -t filesystem,volume autobackup:offsite1
NAME                                    PROPERTY             VALUE                SOURCE
rpool                                   autobackup:offsite1  true                 local
rpool/ROOT                              autobackup:offsite1  true                 inherited from rpool
rpool/ROOT/pve-1                        autobackup:offsite1  true                 inherited from rpool
rpool/data                              autobackup:offsite1  true                 inherited from rpool
rpool/data/vm-100-disk-0                autobackup:offsite1  true                 inherited from rpool
rpool/swap                              autobackup:offsite1  false                local
...
```

### Running zfs_autobackup

There are 2 ways to run the backup, but the endresult is always the same. Its just a matter of security (trust relations between the servers) and preference.

First install the ssh-key on the server that you specify with --ssh-source or --ssh-target.

#### Method 1: Run the script on the backup server and pull the data from the server specfied by --ssh-source. This is usually the preferred way and prevents a hacked server from accesing the backup-data

```console
[root@backup ~]# zfs_autobackup --ssh-source pve.server.com offsite1 backup/pve --progress --verbose --resume

  #### Settings summary
  [Source] Datasets on: pve.server.com
  [Source] Keep the last 10 snapshots.
  [Source] Keep oldest of 1 day, delete after 1 week.
  [Source] Keep oldest of 1 week, delete after 1 month.
  [Source] Keep oldest of 1 month, delete after 1 year.
  [Source] Send all datasets that have 'autobackup:offsite1=true' or 'autobackup:offsite1=child'
  
  [Target] Datasets are local
  [Target] Keep the last 10 snapshots.
  [Target] Keep oldest of 1 day, delete after 1 week.
  [Target] Keep oldest of 1 week, delete after 1 month.
  [Target] Keep oldest of 1 month, delete after 1 year.
  [Target] Receive datasets under: backup/pve
  
  #### Selecting
  [Source] rpool: Selected (direct selection)
  [Source] rpool/ROOT: Selected (inherited selection)
  [Source] rpool/ROOT/pve-1: Selected (inherited selection)
  [Source] rpool/data: Selected (inherited selection)
  [Source] rpool/data/vm-100-disk-0: Selected (inherited selection)
  [Source] rpool/swap: Ignored (disabled)
  
  #### Snapshotting
  [Source] rpool: No changes since offsite1-20200218175435
  [Source] rpool/ROOT: No changes since offsite1-20200218175435
  [Source] rpool/data: No changes since offsite1-20200218175435
  [Source] Creating snapshot offsite1-20200218180123
  
  #### Transferring
  [Target] backup/pve/rpool/ROOT/pve-1@offsite1-20200218175435: resuming
  [Target] backup/pve/rpool/ROOT/pve-1@offsite1-20200218175435: receiving full
  [Target] backup/pve/rpool/ROOT/pve-1@offsite1-20200218175547: receiving incremental
  [Target] backup/pve/rpool/ROOT/pve-1@offsite1-20200218175706: receiving incremental
  [Target] backup/pve/rpool/ROOT/pve-1@offsite1-20200218180049: receiving incremental
  [Target] backup/pve/rpool/ROOT/pve-1@offsite1-20200218180123: receiving incremental
  [Target] backup/pve/rpool/data@offsite1-20200218175435: receiving full
  [Target] backup/pve/rpool/data/vm-100-disk-0@offsite1-20200218175435: receiving full
  ...
```

#### Method 2: Run the script on the server and push the data to the backup server specified by --ssh-target

```console
[root@pve ~]# zfs_autobackup --ssh-target backup.server.com offsite1 backup/pve --progress --verbose --resume

  #### Settings summary
  [Source] Datasets are local
  [Source] Keep the last 10 snapshots.
  [Source] Keep oldest of 1 day, delete after 1 week.
  [Source] Keep oldest of 1 week, delete after 1 month.
  [Source] Keep oldest of 1 month, delete after 1 year.
  [Source] Send all datasets that have 'autobackup:offsite1=true' or 'autobackup:offsite1=child'
  
  [Target] Datasets on: backup.server.com
  [Target] Keep the last 10 snapshots.
  [Target] Keep oldest of 1 day, delete after 1 week.
  [Target] Keep oldest of 1 week, delete after 1 month.
  [Target] Keep oldest of 1 month, delete after 1 year.
  [Target] Receive datasets under: backup/pve
  ...

```

## Tips

* Use ```--verbose``` to see details, otherwise zfs_autobackup will be quiet and only show errors, like a nice unix command.
* Use ```--debug``` if something goes wrong and you want to see the commands that are executed. This will also stop at the first error.
* Use ```--resume``` to be able to resume aborted backups. (not all zfs versions support this)
* Set the ```readonly``` property of the target filesystem to ```on```. This prevents changes on the target side. If there are changes the next backup will fail and will require a zfs rollback. (by using the --rollback option for example)
* Use ```--clear-refreservation``` to save space on your backup server.
* Use ```--clear-mountpoint``` to prevent the target server from mounting the backupped filesystem in the wrong place during a reboot.

### Speeding up SSH and prevent connection flooding

Add this to your ~/.ssh/config:

```console
Host *
    ControlPath ~/.ssh/control-master-%r@%h:%p
    ControlMaster auto
    ControlPersist 3600
```

This will make all your ssh connections persistent and greatly speed up zfs_autobackup for jobs with short intervals.

Thanks @mariusvw :)

### Specifying ssh port or options

The correct way to do this is by creating ~/.ssh/config:

```console
Host smartos04
    Hostname 1.2.3.4
    Port 1234
    user root
    Compression yes
```

This way you can just specify "smartos04" as host.

Also uses compression on slow links.

Look in man ssh_config for many more options.

## Troubleshooting

> ###  cannot receive incremental stream: invalid backup stream

This usually means you've created a new snapshot on the target side during a backup:

* Solution 1: Restart zfs_autobackup and make sure you dont use --resume. If you did use --resume, be sure to "abort" the recveive on the target side with zfs recv -A.
* Solution 2: Destroy the newly created snapshot and restart zfs_autobackup.

> ### internal error: Invalid argument

In some cases (Linux -> FreeBSD) this means certain properties are not fully supported on the target system.

Try using something like: --filter-properties xattr

## Restore example

Restoring can be done with simple zfs commands. For example, use this to restore a specific SmartOS disk image to a temporary restore location:

```console
root@fs1:/home/psy#  zfs send fs1/zones/backup/zfsbackups/smartos01.server.com/zones/a3abd6c8-24c6-4125-9e35-192e2eca5908-disk0@smartos01_fs1-20160110000003 | ssh root@2.2.2.2 "zfs recv zones/restore"
```

After that you can rename the disk image from the temporary location to the location of a new SmartOS machine you've created.

## Monitoring with Zabbix-jobs

You can monitor backups by using my zabbix-jobs script. (<https://github.com/psy0rz/stuff/tree/master/zabbix-jobs>)

Put this command directly after the zfs_backup command in your cronjob:

```console
zabbix-job-status backup_smartos01_fs1 daily $?
```

This will update the zabbix server with the exitcode and will also alert you if the job didnt run for more than 2 days.

## Backuping up a proxmox cluster with HA replication

Due to the nature of proxmox we had to make a few enhancements to zfs_autobackup. This will probably also benefit other systems that use their own replication in combination with zfs_autobackup.

All data under rpool/data can be on multiple nodes of the cluster. The naming of those filesystem is unique over the whole cluster. Because of this we should backup rpool/data of all nodes to the same destination. This way we wont have duplicate backups of the filesystems that are replicated. Because of various options, you can even migrate hosts and zfs_autobackup will be fine. (and it will get the next backup from the new node automaticly)

In the example below we have 3 nodes, named h4, h5 and h6.

The backup will go to a machine named smartos03.

### Preparing the proxmox nodes

On each node select the filesystems as following:

```console
root@h4:~# zfs set autobackup:h4_smartos03=true rpool
root@h4:~# zfs set autobackup:h4_smartos03=false rpool/data
root@h4:~# zfs set autobackup:data_smartos03=child rpool/data
```

* rpool will be backuped the usual way, and is named h4_smartos03. (each node will have a unique name)
* rpool/data will be excluded from the usual backup
* The CHILDREN of rpool/data be selected for a cluster wide backup named data_smartos03. (each node uses the same backup name)

### Preparing the backup server

Extra options needed for proxmox with HA:

* --no-holds: To allow proxmox to destroy our snapshots if a VM migrates to another node.
* --ignore-replicated: To ignore the replicated filesystems of proxmox on the receiving proxmox nodes. (e.g: only backup from the node where the VM is active)

I use the following backup script on the backup server:

```shell
for H in h4 h5 h6; do
  echo "################################### DATA $H"
  #backup data filesystems to a common place
  ./zfs_autobackup --ssh-source root@$H data_smartos03 zones/backup/zfsbackups/pxe1_data --clear-refreservation --clear-mountpoint  --ignore-transfer-errors --strip-path 2 --verbose --resume --ignore-replicated --no-holds $@
  zabbix-job-status backup_$H""_data_smartos03 daily $? >/dev/null 2>/dev/null

  echo "################################### RPOOL $H"
  #backup rpool to own place
  ./zfs_autobackup --ssh-source root@$H $H""_smartos03 zones/backup/zfsbackups/$H --verbose --clear-refreservation --clear-mountpoint  --resume --ignore-transfer-errors $@
  zabbix-job-status backup_$H""_smartos03 daily $? >/dev/null 2>/dev/null
done
```
