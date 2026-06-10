rsync.net has accounts with actual full ZFS support. Note that "A Special "zfs send Capable" Account is Required. (Its basically a VM with full root access)

See https://www.rsync.net/products/zfsintro.html .

This tutorial will show you how to use it.

## Basic backup

We'll start with the basics and then talk about security considerations and encryption.

### Setup ssh keys

To make sure your server can login via ssh automatically:

```console
[root@pve1 ~]# ssh-copy-id root@xxxxx.rsync.net
...
Password for root@xxxxx.rsync.net:

Number of key(s) added: 1
...
```

### Find your target

Login to findout the name of the pool you want to use. (rsync.net created it for you)

```console
[root@pve1 ~]# ssh root@xxxxx.rsync.net
Last login: Thu Jan 13 23:25:48 2022 from ....
FreeBSD 12.2-RELEASE-p3 GENERIC 

Welcome to FreeBSD!

...
root@zh2040b:~ # zfs list
NAME    USED  AVAIL     REFER  MOUNTPOINT
data1   480K  1.04T       96K  /mnt/data1

```

So your target could be data1. (or you can create a dataset like data1/pve1 if you like)

### Select filesystems

Select the filesystems that your want to backup:

```console
[root@pve1 ~]# zfs set autobackup:rsync=true rpool/data/subvol-111-disk-0
```

### Start actual backup

```console
[root@pve1 ~]# zfs-autobackup -v --ssh-target root@xxxxx.rsync.net rsyncnet data1
  zfs-autobackup v3.1.1 - (c)2021 E.H.Eefting (edwin@datux.nl)
  
  Selecting dataset property : autobackup:rsyncnet
  Snapshot format            : rsyncnet-%Y%m%d%H%M%S
  Hold name                  : zfs_autobackup:rsyncnet
  
  #### Source settings
  [Source] Datasets are local
  [Source] Keep the last 10 snapshots.
  [Source] Keep every 1 day, delete after 1 week.
  [Source] Keep every 1 week, delete after 1 month.
  [Source] Keep every 1 month, delete after 1 year.
  
  #### Selecting
  [Source] rpool/data/subvol-111-disk-0: Selected
  
  #### Snapshotting
  [Source] Creating snapshots rsyncnet-20220114000822 in pool rpool
  
  #### Target settings
  [Target] Datasets on: root@xxxxx.rsync.net
  [Target] Keep the last 10 snapshots.
  [Target] Keep every 1 day, delete after 1 week.
  [Target] Keep every 1 week, delete after 1 month.
  [Target] Keep every 1 month, delete after 1 year.
  [Target] Receive datasets under: data1
  
  #### Synchronising
  [Target] data1/rpool/data: Creating filesystem and parents
  [Target] data1/rpool/data/subvol-111-disk-0@rsyncnet-20220114000822: receiving full
>>> Transfer 6% 16MB/s (total 2022MB, 1 minutes left)
```

## Security considerations

The backup VM is just a dedicated VM with you as the only user.

(Note that these considerations are the same for any cloud provider, not just rsync.net)

### Cloud provider 

Although rsync.net is a very professional and trustworthy company, i still will layout the security implications and how you can mitigate some of them with ZFS encryption:

* The rsync staff can login to your VM to for support reasons, so this means they can easily access your data. Even if you use encryption, as long as the key is loaded on the target they could theoretically access your data.
* You are of course allowed to deny them access by removing their ssh key. 
* To make absolutely sure your data is safe without denying anyone access, you should encrypt your data on the source side. (on your server) This will be send over in encrypted form to rsync.net
* Also look at [How zfs-autobackup handles encryption](Encryption)


### Crypto ware

* If you use a push-backup, you are not secured against cryptoware: If they hack your server, they will have access to your rsync.net server as well. 
* You can actually let the rsync.net server login to your server and pull the backup. 
* In case of a pull-backup, your attack surface is bigger: Anyone with access to your rsync.net VM could access your server as well. But cryptoware on your server can never access the backups.


## Upgrading

When it's time to upgrade to a new FreeBSD version, the staff will contact you and can re-image your machine for you. 

All the data stays intact since its on a seperate disk. 

They will also migrate a few important files like .ssh/authorized_keys

## Conclusion

After 2 years of testing i can say rsync.net is a very good solution for offsite ZFS backups.


Note: After i've created this page and did the testing, rsync.net decided to let me keep the account as a way of sponsorship. Thanks :)