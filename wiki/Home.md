Look at the [README.md](../blob/master/README.md) for the introduction.

# Getting started

## Installation

zfs-autobackup creates ZFS snapshots on a "source" machine and then replicates those snapshots to a "target" machine via SSH.

zfs-autobackup may be installed on either the source machine or the target machine, installing it on both is unnecessary.

When installed on the source, zfs-autobackup will push snapshots to the target.  When installed on the target, zfs-autobackup will pull snapshots from the source.

### Using pip

The recommended installation method on most machines is to use [pip](https://pypi.org/project/zfs-autobackup/):

```console
[root@server ~]# pip install --upgrade zfs-autobackup
```

The above command can also be used to upgrade zfs-autobackup to the newest stable version.

To install the latest beta version add the `--pre` option.

### Using pipx

For more modern distributions it might be best to use pipx:
```console
[root@server ~]# pipx install zfs-autobackup
```

To install the latest beta version add `--pip-args=--pre` option.

### Using easy_install

On older machines you might have to use easy_install:

```console
[root@server ~]# easy_install zfs-autobackup
```

### Using the sources

If you don't want to install zfs-autobackup, or want to make some changes to the code, look at [Development](Development)

## Example

In this example, a machine called `backup` is going to create and pull backup snapshots from a machine called `pve01`.

### Setup SSH login

As zfs-autobackup will perform numerous remote commands via ssh, we strongly recommend setting up passwordless login via ssh. This means generating an ssh key on target machine (`backup`) and copying the public ssh key to the source machine (`pve01`).

NOTE: Most examples use root-access on both the source and target. If you want to use a normal user, look [here](https://github.com/psy0rz/zfs_autobackup/wiki/Manual#running-without-root)

#### Generate an SSH key on `backup`

Create an SSH key on the backup machine that runs zfs-autobackup. You only need to do this once.

Use the `ssh-keygen` command and leave the passphrase empty:

```console
root@backup:~# ssh-keygen
Generating public/private rsa key pair.
Enter file in which to save the key (/root/.ssh/id_rsa):
Enter passphrase (empty for no passphrase):
Enter same passphrase again:
...
root@backup:~#
```

#### Copy the SSH key to `pve01`

Now you need to copy the public part of the key to `pve01`

The `ssh-copy-id` command is a handy tool to automate this. It will just ask for your password.

```console
root@backup:~# ssh-copy-id root@pve01
/usr/bin/ssh-copy-id: INFO: Source of key(s) to be installed: "/root/.ssh/id_rsa.pub"
/usr/bin/ssh-copy-id: INFO: attempting to log in with the new key(s), to filter out any that are already installed
Password:

Number of key(s) added: 1

root@backup:~#
```
This allows the backup machine to login to `pve01` as root without password.

### Select filesystems to backup

Next, we specify the filesystems we want to snapshot and replicate by assigning a unique group name to those filesystems.

It's important to choose a unique group name and to use the name consistently.  (Advanced tip: If you have multiple sets of filesystems that you wish to backup differently, you may do this by creating multiple group names.)

In this example, we assign the group name `offsite1` to the filesystems we want to backup.

On the source machine, we set the ```autobackup:offsite1``` zfs property to ```true```, as follows:

```console
[root@pve01 ~]# zfs set autobackup:offsite1=true rpool
[root@pve01 ~]# zfs get -t filesystem,volume autobackup:offsite1
NAME                      PROPERTY             VALUE                SOURCE
rpool                     autobackup:offsite1  true                 local
rpool/ROOT                autobackup:offsite1  true                 inherited from rpool
rpool/ROOT/pve-1          autobackup:offsite1  true                 inherited from rpool
rpool/data                autobackup:offsite1  true                 inherited from rpool
rpool/data/vm-100-disk-0  autobackup:offsite1  true                 inherited from rpool
rpool/data/vm-101-disk-0  autobackup:offsite1  true                 inherited from rpool
rpool/tmp                 autobackup:offsite1  true                 inherited from rpool
```

ZFS properties are ```inherited``` by child datasets. Since we've set the property on the highest dataset, we're essentially backing up the whole pool.

If we don't want to backup everything, we can exclude certain filesystem by setting the property to ```false```:

```console
[root@pve01 ~]# zfs set autobackup:offsite1=false rpool/tmp
[root@pve01 ~]# zfs get -t filesystem,volume autobackup:offsite1
NAME                      PROPERTY             VALUE                SOURCE
rpool                     autobackup:offsite1  true                 local
rpool/ROOT                autobackup:offsite1  true                 inherited from rpool
rpool/ROOT/pve-1          autobackup:offsite1  true                 inherited from rpool
rpool/data                autobackup:offsite1  true                 inherited from rpool
rpool/data/vm-100-disk-0  autobackup:offsite1  true                 inherited from rpool
rpool/data/vm-101-disk-0  autobackup:offsite1  true                 inherited from rpool
rpool/tmp                 autobackup:offsite1  false                local
```

The ```autobackup``` property can have these values:
 * ```true```: Backup the dataset and all its children.
 * ```false```: Don't backup the dataset and all its children. (Exclude the dataset)
 * ```child```: Only backup the children of the dataset, not the dataset itself. 
 * ```parent```: Only backup the dataset, but not the children. (supported in version 3.2 or higher)

(Note: Only use the ```zfs``` command to set these properties.  Do not use the ```zpool``` command.)

To **remove** the property completely, use:
```console
zfs inherit autobackup:offsite1 rpool
```

### Running zfs-autobackup

Run the script on the backup machine and pull the data from the source machine specified by ```--ssh-source```.

```console
[root@backup ~]# zfs-autobackup -v --clear-mountpoint --ssh-source pve01 offsite1 data/backup/pve01
  zfs-autobackup v3.1.1 - (c)2021 E.H.Eefting (edwin@datux.nl)
  
  Selecting dataset property : autobackup:offsite1
  Snapshot format            : offsite1-%Y%m%d%H%M%S
  Hold name                  : zfs_autobackup:offsite1
  
  #### Source settings
  [Source] Datasets on: pve01
  [Source] Keep the last 10 snapshots.
  [Source] Keep every 1 day, delete after 1 week.
  [Source] Keep every 1 week, delete after 1 month.
  [Source] Keep every 1 month, delete after 1 year.
  
  #### Selecting
  [Source] rpool: Selected
  [Source] rpool/ROOT: Selected
  [Source] rpool/ROOT/pve-1: Selected
  [Source] rpool/data: Selected
  [Source] rpool/data/vm-100-disk-0: Selected
  [Source] rpool/data/vm-101-disk-0: Selected
  [Source] rpool/tmp: Excluded
  
  #### Snapshotting
  [Source] Creating snapshots offsite1-20220107131107 in pool rpool
  
  #### Target settings
  [Target] Datasets are local
  [Target] Keep the last 10 snapshots.
  [Target] Keep every 1 day, delete after 1 week.
  [Target] Keep every 1 week, delete after 1 month.
  [Target] Keep every 1 month, delete after 1 year.
  [Target] Receive datasets under: data/backup/pve01
  
  #### Synchronising
  [Target] data/backup/pve01/rpool@offsite1-20220107131107: receiving full
  [Target] data/backup/pve01/rpool/ROOT@offsite1-20220107131107: receiving full
  [Target] data/backup/pve01/rpool/ROOT/pve-1@offsite1-20220107131107: receiving full
  [Target] data/backup/pve01/rpool/data@offsite1-20220107131107: receiving full
  [Target] data/backup/pve01/rpool/data/vm-100-disk-0@offsite1-20220107131107: receiving full
  [Target] data/backup/pve01/rpool/data/vm-101-disk-0@offsite1-20220107131107: receiving full
  
  #### All operations completed successfully
```

### The results

As you might notice, zfs-autobackup preserves the whole parent path of the source.

So `rpool/data/vm100-disk-0` ends up as: `data/backup/pve01/rpool/data/vm-100-disk-0`

Since it's a backup, it's useful to preserve the original structure of the data like this.

### Stripping the path

Since you might think this is ugly, there is the `--strip-path` option. However this can lead to collisions if two source datasets result in the same target paths. Since version 3.1.2 zfs-autobackup will check for this and emit an error.

#### Making source and target paths look the same

If you want your source and target structure to look exactly the same, you have to do the following:

* Select the whole source-pool. In this case: `zfs set autobackup:offsite1=true rpool`
* Use `--strip-path=1`
* Specify target-pool as target-path. In this case: `data`
* You may need to use `--force` option the first time to overwrite the existing target pool.  It is recommended you try with `--test` and without `--force` first (New in v3.1.2).

This configuration will attempt replicate the entire pool from the source to the target. If you wish to exclude specific datasets from being replicated from the source pool, make sure that you do so by running commands such as:

```console
[root@pve01 ~]# zfs set autobackup:offsite1=false rpool/tmp
```

For each dataset you don't want to replicate BEFORE you run zfs-autobackup without `--test` for the first time.

### Pull or push?

Note that this is called a "pull" backup.  The backup (target) machine pulls the backup from the source machine. This is usually the preferred way.

It is also possible to let a source machine push its backup to the target machine. There are security implications to both approaches, as follows:

* With a pull backup, the target machine will have ssh access to the source machine.
* With a push backup, the source machine will have ssh access to the target machine.

If you wish to do a push backup, then you would setup the SSH keys the other way around and use the `--ssh-target` parameter on the source machine.

Note that you can always change the ssh source and target parameters at a later point without any problems.

#### Pull+push (zero trust)

It also possible to use a third server that pulls backups from the source and pushes the data to the target server via one stream. This way the source and target server won't have to be able to reach each other. If one server gets hacked, they can't access the other server.

To do this, you only have to install zfs-autobackup on a third server and use both `--ssh-source` and `--ssh-target` to specify the other source and target servers.

## Local Usage

It is also possible to run zfs-autobackup locally, where you could backup snapshots to a different pool on the same server. This is done by simply omitting the `--ssh-source` and `--ssh-target` parameters. 

For example, let's say you have an additional pool for local backups called `backups`, that's on separate device(s) from your data pools. In this pool, you have a dataset called `autobackup`. You could run the following command (assuming you set the zfs group name to `autobackup:local` on your data filesystems):

> zfs-autobackup -v local backups/autobackup

Combining this with a remote push or pull backup, you could then set the zfs group name on your backup filesystems to something like `autobackup:remote`, then have a second zfs-autobackup job that backs up these snapshots to your remote storage like:

> zfs-autobackup -v --ssh-target root@backupserver remote data/backup/pve01

## Automatic backups

Now every time you run the command, zfs-autobackup will create a new snapshot and replicate your data.

Older snapshots will eventually be deleted, depending on the `--keep-source` and `--keep-target` settings. The defaults are shown above under the 'Settings summary'. Look at [Thinner](Thinner) for more info.

Once you've got the correct settings for your situation, you can just store the command in a cronjob or just create a script and run it manually when you need it.

### Avoid parallel jobs

If a cronjob takes too long, it might start the next zfs-autobackup job, while the previous one hasn't finished. This won't break anything permanently, but backups might fail and the IO load might get even higher. If the jobs keep compounding it might lead to memory exhaustion and server crashes.

Some cron daemons prevent parallel jobs automatically, but you might have to use flock to prevent this. For example:
```
22 * * * * flock -n /var/backups.lock zfs-autobackup backup1 rpool/backup -v --ssh-target=....
```

If you do this, you might miss snapshots of course. If you want to prevent this, split it up in a snapshot-only job (by omitting the target path) and a send-only job that has the --no-snapshot parameter.


## Splitting up snapshot and backup jobs

You might want to make snapshots during the week, and only transfer data during the weekends.

In this case you would run this each weekday: 

> zfs-autobackup -v --ssh-source pve01 offsite1 data/backup/pve01 --no-send

And this on weekend days:

> zfs-autobackup -v --ssh-source pve01 offsite1 data/backup/pve01

You can also create the snapshots in offline mode by using zfs-autobackup as a snapshot tool on the source side. This way the snapshots will always be created, even if the backup server is offline or unreachable.

## Use as a snapshot tool

You can use zfs-autobackup as a standalone snapshot tool.

To do this, simply omit the target-path, as follows:

> zfs-autobackup -v --ssh-source pve01 offsite1

Only use this if you don't want to make any backup at all, or if a target isn't reachable during the snapshotting phase.

If you have offline backups, checkout [[Common-snapshots-and-holds]]

## Monitoring

Don't forget to monitor the results of your backups, look at [Monitoring](Monitoring) for more info.

## Use alongside other snapshot tools

zfs-autobackup can happily co-exist on the same system as other ZFS snapshot tools such as zfs-auto-snapshot if you are already using one. zfs-autobackup will not thin any manually created snapshots or those created by other snapshot tools, it will only thin its own shapshots if you use its `--keep-source` or `--keep-target` options.

## Specifying ssh port or options

The correct way to do this is by creating ~/.ssh/config:

```console
Host smartos04
    Hostname 1.2.3.4
    Port 1234
    user root
```

This way you can just specify "smartos04" as host.

Look in ```man ssh_config``` for many more options.

## Multiple backups of the same data

You can use multiple zfs-autobackup jobs to transfer data to multiple targets. Just make sure that you use different backup names. This way the jobs should not interfere with each other: Each job only removes its own snapshots.

### Using the same backup name

You CAN use the same backup name to transfer data to multiple targets. However in that case it's up to you to make sure that a common snapshot of one backup job isn't deleted by the other job.

One way to do this is to make adjust the --keep-source option or to make sure the backups run at a close enough interval.

To prevent confusion, and to be more flexible, I would advise you to always use easily distinguished names e.g.: autobackup:offsite and autobackup:local, for example.

## Tips

* Use ```--clear-mountpoint``` to prevent all kinds of problems. See [[Mounting]]
* Use ```--debug``` if something goes wrong and you want to see the commands that are executed. This will also stop at the first error.
* Use these only one time if needed: `--force` `--destroy-incompatible` `--rollback`. Don't add them to your script. Try to solve the underlying cause if you keep needing them.
* Set the ```readonly``` property of the target filesystem to ```on```. This prevents changes on the target side. (Due to the nature of ZFS itself, if any changes are made to a dataset on the target machine, then the next backup to that target machine will probably fail. Such a failure can probably be resolved by perfroming a target-side zfs rollback of the affected dataset.) Note that ```readonly``` prevents changes to the CONTENTS of the dataset directly. It's still possible to receive new datasets and manipulate properties etc.
* Use ```--clear-refreservation``` to save space on your backup machine.
* zfs-autobackup holds the common snapshot on the target by default, so you might get "dataset busy" if you try to destroy a snapshot there. (check zfs holds --help or see [here.](https://github.com/psy0rz/zfs_autobackup/wiki/Problems#dataset-is-busy)) On the source side it uses bookmarks since version 4.0, see [[Common-snapshots-and-holds]]

## Restore example

Restoring can be done with simple zfs commands. For example:

```console
root@fs1:/home/psy#  zfs send fs1/zones/backup/zfsbackups/server01/vm01@offset1-20220110230003 | ssh root@2.2.2.2 "zfs recv rpool/restore"
```

## More information

Continue reading the [Full manual](Manual). It will explain in more detail how zfs-autobackup works.

Or jump to:

* [Upgrading to v4.0](Upgrading-to-v4.0)
* [Performance tips (recommended)](Performance)
* [Common problems and errors](Problems)
* [Thinning out obsolete snapshots](Thinner)
* [Replicating ZFS clones](Clones)
* [Handling ZFS encryption](Encryption)
* [Transfer buffering, compression and rate limiting.](Piping)
* [Custom Pre- and post-snapshot commands](PrePost)
* [Monitoring](Monitoring)
* [Proxmox Example](Example%20Proxmox.md)

If you like Alpine linux and want to use it with ZFS, checkout my other project: https://github.com/psy0rz/alpinebox
