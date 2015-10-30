# ZFS autobackup

Introduction
============

ZFS autobackup is used to periodicly backup ZFS filesystems to other locations. This is done using the very effcient zfs send and receive commands.

It has the following features:
* Automaticly selects filesystems to backup by looking at a simple ZFS property.
* Creates consistent snapshots.
* Multiple backups modes: 
 * "push" local data to a backup-server via SSH.
 * "pull" remote data from a server via SSH and backup it locally.
 * Backup local data on the same server.
* Can be scheduled via a simple cronjob or run directly from commandline.
* Backups and snapshots can be named to prevent conflicts. (multiple backups from and to the same filesystems are no problem)
* Always creates new snapshots, even if the previous backup was aborted.
* Checks everything and aborts on errors.
* Ability to 'finish' aborted backups to see what goes wrong.
* Easy to debug and has a test-mode. Actual unix commands are printed.
* Keeps latest X snapshots remote and locally. (default 30, configurable)
* Easy installation:
 * Only one host needs the zfs_autobackup script. The other host just needs ssh and the zfs command.
 * Written in python and uses zfs-commands, no 3rd party dependencys or libraries.

Usage
====
```
usage: zfs_autobackup [-h] [--ssh-source SSH_SOURCE] [--ssh-target SSH_TARGET]
                      [--ssh-cipher SSH_CIPHER] [--keep-source KEEP_SOURCE]
                      [--keep-target KEEP_TARGET] [--finish] [--compress]
                      [--test] [--verbose] [--debug]
                      backup_name target_fs

ZFS autobackup v2.0

positional arguments:
  backup_name           Name of the backup (you should set the zfs property
                        "autobackup:backup-name" to true on filesystems you
                        want to backup
  target_fs             Target filesystem

optional arguments:
  -h, --help            show this help message and exit
  --ssh-source SSH_SOURCE
                        Source host to get backup from. (user@hostname)
                        Default local.
  --ssh-target SSH_TARGET
                        Target host to push backup to. (user@hostname) Default
                        local.
  --ssh-cipher SSH_CIPHER
                        SSH cipher to use (default arcfour128)
  --keep-source KEEP_SOURCE
                        Number of old snapshots to keep on source. Default 30.
  --keep-target KEEP_TARGET
                        Number of old snapshots to keep on target. Default 30.
  --finish              dont create new snapshot, just finish sending current
                        snapshots
  --compress            use compression during zfs send/recv
  --test                dont change anything, just show what would be done
                        (still does all read-only operations)
  --verbose             verbose output
  --debug               debug output (shows user details, decicions that are
                        made and commands that are executed)
```

Example
=======

In this example we're going to backup a SmartOS machine called `smartos01` to our fileserver called `fs1`.

Its important to choose a uniq and consistent backup name. In this case we name our backup: `smartos01_fs1`.

Select filesystems to backup
----------------------------

On the source zfs system set the ```autobackup:smartos01_fs1``` zfs property to true:
```
[root@smartos01 ~]# zfs set autobackup:smartos01_fs1=true zones
[root@smartos01 ~]# zfs get -t filesystem autobackup:smartos01_fs1
NAME                                                PROPERTY                  VALUE                     SOURCE
zones                                               autobackup:smartos01_fs1  true                      local
zones/1eb33958-72c1-11e4-af42-ff0790f603dd          autobackup:smartos01_fs1  true                      inherited from zones
zones/3c71a6cd-6857-407c-880c-09225ce4208e          autobackup:smartos01_fs1  true                      inherited from zones
zones/3c905e49-81c0-4a5a-91c3-fc7996f97d47          autobackup:smartos01_fs1  true                      inherited from zones
...
```

Because we dont want to backup everything, we can exclude certain filesystem by setting the property to false:
```
[root@smartos01 ~]# zfs set autobackup:smartos01_fs1=false zones/backup
[root@smartos01 ~]# zfs get -t filesystem autobackup:smartos01_fs1
NAME                                                PROPERTY                  VALUE                     SOURCE
zones                                               autobackup:smartos01_fs1  true                      local
zones/1eb33958-72c1-11e4-af42-ff0790f603dd          autobackup:smartos01_fs1  true                      inherited from zones
...
zones/backup                                        autobackup:smartos01_fs1  false                     local
zones/backup/fs1                                    autobackup:smartos01_fs1  false                     inherited from zones/backup
...
```

Running zfs_autobackup
----------------------
There are 2 ways to run the backup:

Method 1: Run the script on the backup server and pull the data from the server specfied by --ssh-source. This is usually the preferred way and prevents a hacked server from accesing the backup-data:
```
root@fs1:/home/psy# ./zfs_autobackup --ssh-source root@1.2.3.4 smartos01_fs1 fs1/zones/backup/zfsbackups/smartos01.server.com --verbose --compress
Getting selected source filesystems for backup smartos01_fs1 on root@1.2.3.4
Selected: zones (direct selection)
Selected: zones/1eb33958-72c1-11e4-af42-ff0790f603dd (inherited selection)
Selected: zones/325dbc5e-2b90-11e3-8a3e-bfdcb1582a8d (inherited selection)
...
Ignoring: zones/backup (disabled)
Ignoring: zones/backup/fs1 (disabled)
...
Creating source snapshot smartos01_fs1-20151030203738 on root@1.2.3.4
Getting source snapshot-list from root@1.2.3.4
Getting target snapshot-list from local
Tranferring zones incremental backup between snapshots smartos01_fs1-20151030175345...smartos01_fs1-20151030203738
...
received 1.09MB stream in 1 seconds (1.09MB/sec)
Destroying old snapshots on source
Destroying old snapshots on target
All done
```

Method 2: Run the script on the server and push the data to the backup server specified by --ssh-target:
```
./zfs_autobackup --ssh-target root@2.2.2.2 smartos01_fs1 fs1/zones/backup/zfsbackups/smartos01.server.com --verbose  --compress 
...
All done

```

Monitoring with Zabbix-jobs
===========================

You can monitor backups by using my zabbix-jobs script. (https://github.com/psy0rz/stuff/tree/master/zabbix-jobs)

Put this command directly after the zfs_backup command in your cronjob:
```
zabbix-job-status backup_smartos01_fs1 daily $?
```

This will update the zabbix server with the exitcode and will also alert you if the job didnt run for more than 2 days.
