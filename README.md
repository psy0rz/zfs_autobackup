
# ZFS autobackup

[![Tests](https://github.com/psy0rz/zfs_autobackup/workflows/Regression%20tests/badge.svg)](https://github.com/psy0rz/zfs_autobackup/actions?query=workflow%3A%22Regression+tests%22) [![Coverage Status](https://coveralls.io/repos/github/psy0rz/zfs_autobackup/badge.svg)](https://coveralls.io/github/psy0rz/zfs_autobackup)  [![Python Package](https://github.com/psy0rz/zfs_autobackup/workflows/Upload%20Python%20Package/badge.svg)](https://pypi.org/project/zfs-autobackup/)

## Introduction

This is a tool I wrote to make replicating ZFS datasets easy and reliable.

You can either use it as a **backup** tool, **replication** tool or **snapshot** tool.

You can select what to backup by setting a custom `ZFS property`. This makes it easy to add/remove specific datasets, or just backup your whole pool.

Other settings are just specified on the commandline: Simply setup and test your zfs-autobackup command and  fix all the issues you might encounter. When you're done you can just copy/paste your command to a cron or script.

Since its using ZFS commands, you can see what its actually doing by specifying `--debug`. This also helps a lot if you run into some strange problem or error. You can just copy-paste the command that fails and play around with it on the commandline. (something I missed in other tools)

An important feature thats missing from other tools is a reliable `--test` option: This allows you to see what zfs-autobackup will do and tune your parameters. It will do everything, except make changes to your system.

zfs-autobackup tries to be the easiest to use backup tool for zfs, with the most features.

## Features

* Works across operating systems: Tested with **Linux**, **FreeBSD/FreeNAS** and **SmartOS**.
* Low learning curve: no complex daemons or services, no additional software or networking needed. (Only read this page)   
* Plays nicely with existing replication systems. (Like Proxmox HA)
* Automatically selects filesystems to backup by looking at a simple ZFS property. (recursive)
* Creates consistent snapshots. (takes all snapshots at once, atomicly.)
* Multiple backups modes:
  * Backup local data on the same server.
  * "push" local data to a backup-server via SSH.
  * "pull" remote data from a server via SSH and backup it locally.
  * Or even pull data from a server while pushing the backup to another server. (Zero trust between source and target server)
* Can be scheduled via a simple cronjob or run directly from commandline.
* Supports resuming of interrupted transfers.
* ZFS encryption support: Can decrypt / encrypt or even re-encrypt datasets during transfer.
* Multiple backups from and to the same datasets are no problem.
* Creates the snapshot before doing anything else. (assuring you at least have a snapshot if all else fails)
* Checks everything but tries continue on non-fatal errors when possible. (Reports error-count when done)
* Ability to manually 'finish' failed backups to see whats going on.
* Easy to debug and has a test-mode. Actual unix commands are printed.
* Uses **progressive thinning** for older snapshots.
* Uses zfs-holds on important snapshots so they cant be accidentally destroyed.
* Automatic resuming of failed transfers.
* Can continue from existing common snapshots. (e.g. easy migration)
* Gracefully handles datasets that no longer exist on source.
* Easy installation:
  * Just install zfs-autobackup via pip, or download it manually.
  * Only needs to be installed on one side.
  * Written in python and uses zfs-commands, no 3rd party dependency's or libraries needed.
  * No separate config files or properties. Just one zfs-autobackup command you can copy/paste in your backup script.

## Installation

You only need to install zfs-autobackup on the side that initiates the backup. The other side doesnt need any extra configration.

### Using pip

The recommended way on most servers is to use [pip](https://pypi.org/project/zfs-autobackup/):

```console
[root@server ~]# pip install --upgrade zfs-autobackup
```

This can also be used to upgrade zfs-autobackup to the newest stable version.

### Using easy_install

On older servers you might have to use easy_install

```console
[root@server ~]# easy_install zfs-autobackup
```

## Example

In this example we're going to backup a machine called `server1` to a machine called `backup`.

### Setup SSH login

zfs-autobackup needs passwordless login via ssh. This means generating an ssh key and copying it to the remote server.

#### Generate SSH key on `backup`

On the backup-server that runs zfs-autobackup you need to create an SSH key. You only need to do this once.

Use the `ssh-keygen` command and leave the passphrase empty:

```console
root@backup:~# ssh-keygen
Generating public/private rsa key pair.
Enter file in which to save the key (/root/.ssh/id_rsa):
Enter passphrase (empty for no passphrase):
Enter same passphrase again:
Your identification has been saved in /root/.ssh/id_rsa.
Your public key has been saved in /root/.ssh/id_rsa.pub.
The key fingerprint is:
SHA256:McJhCxvaxvFhO/3e8Lf5gzSrlTWew7/bwrd2U2EHymE root@backup
The key's randomart image is:
+---[RSA 2048]----+
|    + =          |
|   + X *    E .  |
|  . = B +  o o . |
|   .   o +  o  o.|
|        S o   .oo|
|         . + o= +|
|          . ++==.|
|            .+o**|
|           .. +B@|
+----[SHA256]-----+
root@backup:~#
```

#### Copy SSH key to `server1`

Now you need to copy the public part of the key to `server1`

The `ssh-copy-id` command is a handy tool to automate this. It will just ask for your password.

```console
root@backup:~# ssh-copy-id root@server1.server.com
/usr/bin/ssh-copy-id: INFO: Source of key(s) to be installed: "/root/.ssh/id_rsa.pub"
/usr/bin/ssh-copy-id: INFO: attempting to log in with the new key(s), to filter out any that are already installed
/usr/bin/ssh-copy-id: INFO: 1 key(s) remain to be installed -- if you are prompted now it is to install the new keys
Password:

Number of key(s) added: 1

Now try logging into the machine, with:   "ssh 'root@server1.server.com'"
and check to make sure that only the key(s) you wanted were added.

root@backup:~#
```
This allows the backup-server to login to `server1` as root without password.

### Select filesystems to backup

Its important to choose a unique and consistent backup name. In this case we name our backup: `offsite1`.

On the source zfs system set the ```autobackup:offsite1``` zfs property to true:

```console
[root@server1 ~]# zfs set autobackup:offsite1=true rpool
[root@server1 ~]# zfs get -t filesystem,volume autobackup:offsite1
NAME                                    PROPERTY             VALUE                SOURCE
rpool                                   autobackup:offsite1  true                 local
rpool/ROOT                              autobackup:offsite1  true                 inherited from rpool
rpool/ROOT/server1-1                    autobackup:offsite1  true                 inherited from rpool
rpool/data                              autobackup:offsite1  true                 inherited from rpool
rpool/data/vm-100-disk-0                autobackup:offsite1  true                 inherited from rpool
rpool/swap                              autobackup:offsite1  true                 inherited from rpool
...
```

ZFS properties are ```inherited``` by child datasets. Since we've set the property on the highest dataset, we're essentially backupping the whole pool.

Because we don't want to backup everything, we can exclude certain filesystem by setting the property to false:

```console
[root@server1 ~]# zfs set autobackup:offsite1=false rpool/swap
[root@server1 ~]# zfs get -t filesystem,volume autobackup:offsite1
NAME                                    PROPERTY             VALUE                SOURCE
rpool                                   autobackup:offsite1  true                 local
rpool/ROOT                              autobackup:offsite1  true                 inherited from rpool
rpool/ROOT/server1-1                    autobackup:offsite1  true                 inherited from rpool
rpool/data                              autobackup:offsite1  true                 inherited from rpool
rpool/data/vm-100-disk-0                autobackup:offsite1  true                 inherited from rpool
rpool/swap                              autobackup:offsite1  false                local
...
```

The autobackup-property can have 3 values:
 * ```true```: Backup the dataset and all its children 
 * ```false```: Dont backup the dataset and all its children. (used to exclude certain datasets)
 * ```child```: Only backup the children off the dataset, not the dataset itself.

Only use the zfs-command to set these properties, not the zpool command. 

### Running zfs-autobackup

Run the script on the backup server and pull the data from the server specified by --ssh-source.

```console
[root@backup ~]# zfs-autobackup --ssh-source server1.server.com offsite1 backup/server1 --progress --verbose

  #### Settings summary
  [Source] Datasets on: server1.server.com
  [Source] Keep the last 10 snapshots.
  [Source] Keep every 1 day, delete after 1 week.
  [Source] Keep every 1 week, delete after 1 month.
  [Source] Keep every 1 month, delete after 1 year.
  [Source] Send all datasets that have 'autobackup:offsite1=true' or 'autobackup:offsite1=child'

  [Target] Datasets are local
  [Target] Keep the last 10 snapshots.
  [Target] Keep every 1 day, delete after 1 week.
  [Target] Keep every 1 week, delete after 1 month.
  [Target] Keep every 1 month, delete after 1 year.
  [Target] Receive datasets under: backup/server1

  #### Selecting
  [Source] rpool: Selected (direct selection)
  [Source] rpool/ROOT: Selected (inherited selection)
  [Source] rpool/ROOT/server1-1: Selected (inherited selection)
  [Source] rpool/data: Selected (inherited selection)
  [Source] rpool/data/vm-100-disk-0: Selected (inherited selection)
  [Source] rpool/swap: Ignored (disabled)

  #### Snapshotting
  [Source] rpool: No changes since offsite1-20200218175435
  [Source] rpool/ROOT: No changes since offsite1-20200218175435
  [Source] rpool/data: No changes since offsite1-20200218175435
  [Source] Creating snapshot offsite1-20200218180123

  #### Sending and thinning
  [Target] backup/server1/rpool/ROOT/server1-1@offsite1-20200218175435: receiving full
  [Target] backup/server1/rpool/ROOT/server1-1@offsite1-20200218175547: receiving incremental
  [Target] backup/server1/rpool/ROOT/server1-1@offsite1-20200218175706: receiving incremental
  [Target] backup/server1/rpool/ROOT/server1-1@offsite1-20200218180049: receiving incremental
  [Target] backup/server1/rpool/ROOT/server1-1@offsite1-20200218180123: receiving incremental
  [Target] backup/server1/rpool/data@offsite1-20200218175435: receiving full
  [Target] backup/server1/rpool/data/vm-100-disk-0@offsite1-20200218175435: receiving full
  ...
```

Note that this is called a "pull" backup: The backup server pulls the backup from the server. This is usually the preferred way.

Its also possible to let a server push its backup to the backup-server. However this has security implications. In that case you would setup the SSH keys the other way around and use the --ssh-target parameter on the server.

### Automatic backups

Now every time you run the command, zfs-autobackup will create a new snapshot and replicate your data.

Older snapshots will eventually be deleted, depending on the `--keep-source` and `--keep-target` settings. (The defaults are shown above under the 'Settings summary')

Once you've got the correct settings for your situation, you can just store the command in a cronjob.

Or just create a script and run it manually when you need it.

## Use as snapshot tool

You can use zfs-autobackup to only make snapshots.

Just dont specify the target-path:
```console
root@ws1:~# zfs-autobackup test --verbose
  zfs-autobackup v3.0 - Copyright 2020 E.H.Eefting (edwin@datux.nl)

  #### Source settings
  [Source] Datasets are local
  [Source] Keep the last 10 snapshots.
  [Source] Keep every 1 day, delete after 1 week.
  [Source] Keep every 1 week, delete after 1 month.
  [Source] Keep every 1 month, delete after 1 year.
  [Source] Selects all datasets that have property 'autobackup:test=true' (or childs of datasets that have 'autobackup:test=child')

  #### Selecting
  [Source] test_source1/fs1: Selected (direct selection)
  [Source] test_source1/fs1/sub: Selected (inherited selection)
  [Source] test_source2/fs2: Ignored (only childs)
  [Source] test_source2/fs2/sub: Selected (inherited selection)

  #### Snapshotting
  [Source] Creating snapshots test-20200710125958 in pool test_source1
  [Source] Creating snapshots test-20200710125958 in pool test_source2

  #### Thinning source
  [Source] test_source1/fs1@test-20200710125948: Destroying
  [Source] test_source1/fs1/sub@test-20200710125948: Destroying
  [Source] test_source2/fs2/sub@test-20200710125948: Destroying

  #### All operations completed successfully
  (No target_path specified, only operated as snapshot tool.)
```

This also allows you to make several snapshots during the day, but only backup the data at night when the server is not busy.

## Thinning out obsolete snapshots

The thinner is the thing that destroys old snapshots on the source and target.

The thinner operates "stateless": There is nothing in the name or properties of a snapshot that indicates how long it will be kept. Everytime zfs-autobackup runs, it will look at the timestamp of all the existing snapshots. From there it will determine which snapshots are obsolete according to your schedule. The advantage of this stateless system is that you can always change the schedule.

Note that the thinner will ONLY destroy snapshots that are matching the naming pattern of zfs-autobackup. If you use `--other-snapshots`, it wont destroy those snapshots after replicating them to the target.

### Destroying missing datasets

When a dataset has been destroyed or deselected on the source, but still exists on the target we call it a missing dataset. Missing datasets will be still thinned out according to the schedule.

The final snapshot will never be destroyed, unless you specify a **deadline** with the `--destroy-missing` option:

In that case it will look at the last snapshot we took and determine if is older than the deadline you specified. e.g: `--destroy-missing 30d` will start destroying things 30 days after the last snapshot.

#### After the deadline

When the deadline is passed, all our snapshots, except the last one will be destroyed. Irregardless of the normal thinning schedule.

The dataset has to have the following properties to be finally really destroyed:

* The dataset has no direct child-filesystems or volumes.
* The only snapshot left is the last one created by zfs-autobackup.
* The remaining snapshot has no clones.

### Thinning schedule

The default thinning schedule is: `10,1d1w,1w1m,1m1y`.

The schedule consists of multiple rules separated by a `,`

A plain number specifies how many snapshots you want to always keep, regardless of time or interval.

The format of the other rules is: `<Interval><TTL>`.

* Interval: The minimum interval between the snapshots. Snapshots with intervals smaller than this will be destroyed.
* TTL: The maximum time to life time of a snapshot, after that they will be destroyed.
* These are the time units you can use for interval and TTL:
  * `y`: Years
  * `m`: Months
  * `d`: Days
  * `h`: Hours
  * `min`: Minutes
  * `s`: Seconds

Since this might sound very complicated, the `--verbose` option will show you what it all means:

```console
  [Source] Keep the last 10 snapshots.
  [Source] Keep every 1 day, delete after 1 week.
  [Source] Keep every 1 week, delete after 1 month.
  [Source] Keep every 1 month, delete after 1 year.
```

A snapshot will only be destroyed if it not needed anymore by ANY of the rules.

You can specify as many rules as you need. The order of the rules doesn't matter.

Keep in mind its up to you to actually run zfs-autobackup often enough: If you want to keep hourly snapshots, you have to make sure you at least run it every hour.

However, its no problem if you run it more or less often than that: The thinner will still keep an optimal set of snapshots to match your schedule as good as possible.

If you want to keep as few snapshots as possible, just specify 0. (`--keep-source=0` for example)

If you want to keep ALL the snapshots, just specify a very high number.

### More details about the Thinner

We will give a practical example of how the thinner operates.

Say we want have 3 thinner rules:

* We want to keep daily snapshots for 7 days.
* We want to keep weekly snapshots for 4 weeks.
* We want to keep monthly snapshots for 12 months.

So far we have taken 4 snapshots at random moments:

![thinner example](https://raw.githubusercontent.com/psy0rz/zfs_autobackup/master/doc/thinner.png)

For every rule, the thinner will divide the timeline in blocks and assign each snapshot to a block.

A block can only be assigned one snapshot: If multiple snapshots fall into the same block, it only assigns it to the oldest that we want to keep.

The colors show to which block a snapshot belongs:

* Snapshot 1: This snapshot belongs to daily block 1, weekly block 0 and monthly block 0. However the daily block is too old.
* Snapshot 2: Since weekly block 0 and monthly block 0 already have a snapshot, it only belongs to daily block 4.
* Snapshot 3: This snapshot belongs to daily block 8 and weekly block 1.
* Snapshot 4: Since daily block 8 already has a snapshot, this one doesn't belong to anything and can be deleted right away. (it will be keeped for now since its the last snapshot)

zfs-autobackup will re-evaluate this on every run: As soon as a snapshot doesn't belong to any block anymore it will be destroyed.

Snapshots on the source that still have to be send to the target wont be destroyed off course. (If the target still wants them, according to the target schedule)

## How zfs-autobackup handles encryption

In normal operation datasets are transferred unaltered:

* Source datasets that are encrypted will be send over as such and stay encrypted at the target side. (In ZFS this is called raw-mode) You dont need keys at the target side if you dont want to access the data.
* Source datasets that are plain will stay that way on the target. (Even if the specified target-path IS encrypted.) 

Basically you dont have to do anything or worry about anything. 

### Decrypting/encrypting

Things get different if you want to change the encryption-state of a dataset during transfer:

* If you want to decrypt encrypted datasets before sending them, you should use the `--decrypt` option. Datasets will then be stored plain at the target.
* If you want to encrypt plain datasets when they are received, you should use the `--encrypt` option. Datasets will then be stored encrypted at the target. (Datasets that are already encrypted will still be sent over unaltered in raw-mode.) 
* If you also want re-encrypt encrypted datasets with the target-side encryption you can use both options. 

Note 1: The --encrypt option will rely on inheriting encryption parameters from the parent datasets on the target side. You are responsible for setting those up and loading the keys. So --encrypt is no guarantee for encryption: If you dont set it up, it cant encrypt.

Note 2: Decide what you want at an early stage: If you change the --encrypt or --decrypt parameter after the inital sync you might get weird and wonderfull errors. (nothing dangerous)

I'll add some tips when the issues start to get in on github. :)

## Tips

* Use ```--debug``` if something goes wrong and you want to see the commands that are executed. This will also stop at the first error.
* You can split up the snapshotting and sending tasks by creating two cronjobs. Create a separate snapshotter-cronjob by just omitting target-path.
* Set the ```readonly``` property of the target filesystem to ```on```. This prevents changes on the target side. (Normally, if there are changes the next backup will fail and will require a zfs rollback.) Note that readonly means you cant change the CONTENTS of the dataset directly. Its still possible to receive new datasets and manipulate properties etc.
* Use ```--clear-refreservation``` to save space on your backup server.
* Use ```--clear-mountpoint``` to prevent the target server from mounting the backupped filesystem in the wrong place during a reboot.

### Performance tips

If you have a large number of datasets its important to keep the following tips in mind.

#### Some statistics

To get some idea of how fast zfs-autobackup is, I did some test on my laptop, with a SKHynix_HFS512GD9TNI-L2B0B disk. I'm using zfs 2.0.2.  

I created 100 empty datasets and measured the total runtime of zfs-autobackup. I used all the performance tips below. (--no-holds, --allow-empty, ssh ControlMaster)

* without ssh: 15 seconds. (>6 datasets/s)
* either ssh-target or ssh-source=localhost: 20 seconds (5 datasets/s)
* both ssh-target and ssh-source=localhost: 24 seconds (4 datasets/s)

To be bold I created 2500 datasets, but that also was no problem. So it seems it should be possible to use zfs-autobackup with thousands of datasets.

If you need more performance let me know.

NOTE: There is actually a performance regression in ZFS version 2: https://github.com/openzfs/zfs/issues/11560 Use --no-progress as workaround.

#### Less work

You can make zfs-autobackup generate less work by using --no-holds and --allow-empty.

This saves a lot of extra zfs-commands per dataset.

#### Speeding up SSH

You can make your ssh connections persistent and greatly speed up zfs-autobackup:

On the backup-server add this to your ~/.ssh/config:

```console
Host *
    ControlPath ~/.ssh/control-master-%r@%h:%p
    ControlMaster auto
    ControlPersist 3600
```

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

## Usage

(NOTE: Quite a lot has changed since the current stable version 3.0. The page your are viewing is for upcoming version 3.1 which is still in beta.)

```console
usage: zfs-autobackup [-h] [--ssh-config CONFIG-FILE] [--ssh-source USER@HOST] [--ssh-target USER@HOST] [--keep-source SCHEDULE] [--keep-target SCHEDULE] [--other-snapshots] [--no-snapshot] [--no-send]
                   [--no-thinning] [--no-holds] [--min-change BYTES] [--allow-empty] [--ignore-replicated] [--strip-path N] [--clear-refreservation] [--clear-mountpoint] [--filter-properties PROPERY,...]
                   [--set-properties PROPERTY=VALUE,...] [--rollback] [--destroy-incompatible] [--destroy-missing SCHEDULE] [--ignore-transfer-errors] [--decrypt] [--encrypt] [--test] [--verbose] [--debug]
                   [--debug-output] [--progress] [--send-pipe COMMAND] [--recv-pipe COMMAND]
                   backup-name [target-path]

zfs-autobackup v3.1-beta3 - Copyright 2020 E.H.Eefting (edwin@datux.nl)

positional arguments:
  backup-name           Name of the backup (you should set the zfs property "autobackup:backup-name" to true on filesystems you want to backup
  target-path           Target ZFS filesystem (optional: if not specified, zfs-autobackup will only operate as snapshot-tool on source)

optional arguments:
  -h, --help            show this help message and exit
  --ssh-config CONFIG-FILE
                        Custom ssh client config
  --ssh-source USER@HOST
                        Source host to get backup from.
  --ssh-target USER@HOST
                        Target host to push backup to.
  --keep-source SCHEDULE
                        Thinning schedule for old source snapshots. Default: 10,1d1w,1w1m,1m1y
  --keep-target SCHEDULE
                        Thinning schedule for old target snapshots. Default: 10,1d1w,1w1m,1m1y
  --other-snapshots     Send over other snapshots as well, not just the ones created by this tool.
  --no-snapshot         Don't create new snapshots (useful for finishing uncompleted backups, or cleanups)
  --no-send             Don't send snapshots (useful for cleanups, or if you want a serperate send-cronjob)
  --no-thinning         Do not destroy any snapshots.
  --no-holds            Don't hold snapshots. (Faster. Allows you to destroy common snapshot.)
  --min-change BYTES    Number of bytes written after which we consider a dataset changed (default 1)
  --allow-empty         If nothing has changed, still create empty snapshots. (same as --min-change=0)
  --ignore-replicated   Ignore datasets that seem to be replicated some other way. (No changes since lastest snapshot. Useful for proxmox HA replication)
  --strip-path N        Number of directories to strip from target path (use 1 when cloning zones between 2 SmartOS machines)
  --clear-refreservation
                        Filter "refreservation" property. (recommended, safes space. same as --filter-properties refreservation)
  --clear-mountpoint    Set property canmount=noauto for new datasets. (recommended, prevents mount conflicts. same as --set-properties canmount=noauto)
  --filter-properties PROPERY,...
                        List of properties to "filter" when receiving filesystems. (you can still restore them with zfs inherit -S)
  --set-properties PROPERTY=VALUE,...
                        List of propererties to override when receiving filesystems. (you can still restore them with zfs inherit -S)
  --rollback            Rollback changes to the latest target snapshot before starting. (normally you can prevent changes by setting the readonly property on the target_path to on)
  --destroy-incompatible
                        Destroy incompatible snapshots on target. Use with care! (implies --rollback)
  --destroy-missing SCHEDULE
                        Destroy datasets on target that are missing on the source. Specify the time since the last snapshot, e.g: --destroy-missing 30d
  --ignore-transfer-errors
                        Ignore transfer errors (still checks if received filesystem exists. useful for acltype errors)
  --decrypt             Decrypt data before sending it over.
  --encrypt             Encrypt data after receiving it.
  --test                dont change anything, just show what would be done (still does all read-only operations)
  --verbose             verbose output
  --debug               Show zfs commands that are executed, stops after an exception.
  --debug-output        Show zfs commands and their output/exit codes. (noisy)
  --progress            show zfs progress output. Enabled automaticly on ttys. (use --no-progress to disable)
  --send-pipe COMMAND   pipe zfs send output through COMMAND
  --recv-pipe COMMAND   pipe zfs recv input through COMMAND

Full manual at: https://github.com/psy0rz/zfs_autobackup
```

## Troubleshooting

### It keeps asking for my SSH password

You forgot to setup automatic login via SSH keys, look in the example how to do this.

### It says 'cannot receive incremental stream: invalid backup stream'

This usually means you've created a new snapshot on the target side during a backup. If you restart zfs-autobackup, it will automaticly abort the invalid partially received snapshot and start over.

### It says 'cannot receive incremental stream: destination has been modified since most recent snapshot'

This means files have been modified on the target side somehow. 

You can use --rollback to automaticly rollback such changes. Also try destroying the target dataset and using --clear-mountpoint on the next run. This way it wont get mounted.

### It says 'internal error: Invalid argument'

In some cases (Linux -> FreeBSD) this means certain properties are not fully supported on the target system.

Try using something like: --filter-properties xattr or --ignore-transfer-errors. 

### zfs receive fails, but snapshot seems to be received successful.

This happens if you transfer between different Operating systems/zfs versions or feature sets.

Try using the --ignore-transfer-errors option. This will ignore the error. It will still check if the snapshot is actually received correctly.

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

This will update the zabbix server with the exit code and will also alert you if the job didn't run for more than 2 days.

## Backup a proxmox cluster with HA replication

Due to the nature of proxmox we had to make a few enhancements to zfs-autobackup. This will probably also benefit other systems that use their own replication in combination with zfs-autobackup.

All data under rpool/data can be on multiple nodes of the cluster. The naming of those filesystem is unique over the whole cluster. Because of this we should backup rpool/data of all nodes to the same destination. This way we wont have duplicate backups of the filesystems that are replicated. Because of various options, you can even migrate hosts and zfs-autobackup will be fine. (and it will get the next backup from the new node automatically)

In the example below we have 3 nodes, named pve1, pve2 and pve3.

### Preparing the proxmox nodes

No preparation is needed, the script will take care of everything. You only need to setup the ssh keys, so that the backup server can access the proxmox server.

TIP: make sure your backup server is firewalled and cannot be reached from any production machine.

### SSH config on backup server

I use ~/.ssh/config to specify how to reach the various hosts.

In this example we are making an offsite copy and use portforwarding to reach the proxmox machines:
```
Host *
    ControlPath ~/.ssh/control-master-%r@%h:%p
    ControlMaster auto
    ControlPersist 3600
    Compression yes

Host pve1
    Hostname some.host.com
    Port 10001

Host pve2
    Hostname some.host.com
    Port 10002

Host pve3
    Hostname some.host.com
    Port 10003
```

### Backup script

I use the following backup script on the backup server.

Adjust the variables HOSTS TARGET and NAME to your needs.

```shell
#!/bin/bash

HOSTS="pve1 pve2 pve3"
TARGET=rpool/pvebackups
NAME=prox

zfs create -p $TARGET/data &>/dev/null
for HOST in $HOSTS; do

  echo "################################### RPOOL $HOST"

  # enable backup
  ssh $HOST "zfs set autobackup:rpool_$NAME=child rpool/ROOT"

  #backup rpool to specific directory per host
  zfs create -p $TARGET/rpools/$HOST &>/dev/null
  zfs-autobackup --keep-source=1d1w,1w1m --ssh-source $HOST rpool_$NAME $TARGET/rpools/$HOST --clear-mountpoint --clear-refreservation --ignore-transfer-errors --strip-path 2 --verbose   --no-holds   $@

  zabbix-job-status backup_$HOST""_rpool_$NAME daily $? >/dev/null 2>/dev/null


  echo "################################### DATA $HOST"

  # enable backup
  ssh $HOST "zfs set autobackup:data_$NAME=child rpool/data"

  #backup data filesystems to a common directory
  zfs-autobackup --keep-source=1d1w,1w1m --ssh-source $HOST data_$NAME $TARGET/data --clear-mountpoint --clear-refreservation --ignore-transfer-errors --strip-path 2 --verbose  --ignore-replicated --min-change 200000 --no-holds   $@

  zabbix-job-status backup_$HOST""_data_$NAME daily $? >/dev/null 2>/dev/null

done
```

This script will also send the backup status to Zabbix. (if you've installed my zabbix-job-status script https://github.com/psy0rz/stuff/tree/master/zabbix-jobs)

# Sponsor list

This project was sponsorred by:

* JetBrains (Provided me with a license for their whole professional product line, https://www.jetbrains.com/pycharm/ )
