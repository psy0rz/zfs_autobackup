# Checking backups with zfs-check

This tool is part of zfs-autobackup v3.2-alpha2 and higher. 

Get it with:
```
pip install zfs-autobackup --pre --upgrade
```

## What does it do?

zfs-check is a tool to generate checksum streams from zfs datasets. (it also can be used on regular block devices and files)

Its kind of like sha1sum, but incremental: It generates a sha1 hash per "chunk" of data. This allows you to use it on huge datasets and only do a partial checks of the data.

## Why?

A tool like this wouldn't seem necessary for ZFS: ZFS has full check summing, so you can be 100% sure the data doesn't get corrupted, right?

While this is true to a certain extend, it IS possible that data gets corrupted during transfer with zfs send/recv. This can happen because of certain bugs in ZFS. For example: https://github.com/openzfs/zfs/issues/12762 and https://github.com/openzfs/zfs/issues/6224

In such cases the checksums are still OK, but they are of the wrong (corrupt) data.

So a tool to actually compare the data of 2 datasets was needed, hence zfs-check.

## How to use it?

In its simplest form you can just use it on a directory like this:

```shell
[root@pve1 ~]# zfs-check /bin --verbose |head -20
  zfs-check v3.2-alpha2 - (c)2022 E.H.Eefting (edwin@datux.nl)
  
  Target               : /bin
  Block size           : 4096 bytes
  Block count          : 25600
  Effective chunk size : 104857600 bytes
  Skip chunk count     : 0 (checks 100.00% of data)
  
smbtree	0	cec00f01df1064a98640e01b50c4030259c655bc
ls	0	605d772c98dd91c4df9a9200e63c5edf10cff4ce
cifsdd	0	11348b768b5f14234863c02392dcbe749d34bf91
dh_bash-completion	0	9d17e0e071892ef43f1ae3fdb5abd40b5bb45252
swtpm_localca	0	83b427c1ae0d91d46cd49db8ae31e2425e5de55a
dpkg-mergechangelogs	0	13ec450b69b525bc510bcf15f2409a647928fc43
true	0	f490cefeec06ba345a5e53ec9814a9763a3330e9
...
```

The 0 in this output means its the checksum of chunk zero of each file. Since the default chunksize is 100MB, all the small files in this example have just one chunk.

### Comparing with --check

The output of this can then be used as input on another zfs-check. In this example we detected an error:

```shell
[root@pve1 ~]# zfs-check /bin > checksums
[root@pve1 ~]# zfs-check /bin --check checksums
aa-enabled: Chunk 0 failed: c2a2be25e6c3a5d89005028ea37a61771489710a c2a2be25e6c3a5d89005028ea37a61771489710f
```

It displays the expected sha1sum vs the actual sha1sum)

## Using it on ZFS snapshots

You can just specify a snapshot of a volume or filesystem. zfs-check will mount it and check it just like the above examples. (Use `--debug` if you want to see how it does this.)

On a ZFS volume:
```
[root@pve1 ~]# zfs-check rpool/data/vm-101-disk-0@kantoor_offsite-20220308020453 
0	87a193d73a27aceb38334eca51d180493c9a2348
1	92559a75e61b021e6a3a351a6b241d7440b79d55
2	6abb3ec919ccbe6ac36580cc43f34af80280ae18
...
```

On a ZFS dataset:
```
[root@pve1 ~]# zfs-check rpool/data/subvol-104-disk-0@kantoor_offsite-20220308020453 
run/resolvconf/resolv.conf	0	c3f9736e9af7bd0885578859a50b205c8fa5fc8e
run/resolvconf/interface/original.resolvconf	0	c3f9736e9af7bd0885578859a50b205c8fa5fc8e
run/samba/names.tdb	0	3dddf16c3899dcf79c0beb636520cc58c86c4ef2
run/samba/gencache_notrans.tdb	0	7c1499d1a78a24d08dbeaeb7bf93ffdc0b0fac41
run/samba/mutex.tdb	0	3dddf16c3899dcf79c0beb636520cc58c86c4ef2
run/samba/upgrades/smb.conf	0	ffc2469dd94b7772c2f1689a43e5bacf62bdd0d1
run/samba/msg.lock/13960	0	21c27e175354df9df55b9b3d3500482b2ea99161
run/samba/msg.lock/6124	0	d6b22ab7ca0ac99d3d74eb58499ccfd2fc85c426
...
```

## Using it to compare a remote and local ZFS dataset

You can use it via a simple SSH pipe to compare to datasets:
```shell
[root@pve1 ~]# zfs-check rpool/data/vm-101-disk-0@offsite-20220308020453 | ssh backupserver1 "zfs-check backup/pve1/rpool/data/vm-101-disk-0@offsite-20220308020453 --check"

```

## Only checking a small sample of the total data

You can use the `--skip` option to skip a certain amount of chunks. To only check 10% of your data use --skip 9. (It will check 1 block and then skip 9 blocks)

```shell
[root@pve1 ~]# zfs-check rpool/data/vm-101-disk-0@offsite-20220308020453 --skip 9 --verbose
  zfs-check v3.2-alpha2 - (c)2022 E.H.Eefting (edwin@datux.nl)
  
  Target               : rpool/data/vm-101-disk-0@kantoor_offsite-20220308020453
  Block size           : 4096 bytes
  Block count          : 25600
  Effective chunk size : 104857600 bytes
  Skip chunk count     : 9 (checks 10.00% of data)
  
0	87a193d73a27aceb38334eca51d180493c9a2348
10	2c2ceccb5ec5574f791d45b63c940cff20550f9a
20	2c2ceccb5ec5574f791d45b63c940cff20550f9a
30	6fdee2a737a0b4db519a70dc2ecd765a90a10bce
```

Normally you use this on the "generating" side.

You can also use it on the "checking" side: It will then just skip this number of hashes. This is usefull if you already generated a 100% list of checksums, but you only want to compare a smaller sample of it.

Note: Keep in mind that if you use --skip on both sides, it will skip 2 times. If you use --skip=9 on both sides, the sender will only send 10% and the checker will only check 10% of that amount. (resulting in a coverage of 1%)


## Using it automaticly with zfs-autoverify

**Work in progress**

This tool will use zfs-check to automaticly verify your backups that where made with zfs-autobackup.


 