## Mounting ZFS datasets

TLDR: Always use `--clear-mountpoint` unless you have good reasons not to.

When a target dataset is mounted, in some cases ZFS might modify a parent dataset. 

On the next backup run you'll get this dreaded message:

```
 cannot receive incremental stream: destination has been modified since most recent snapshot
```

If you then proceed to use `--rollback` or `--force` things might get weird:  ZFS will yank away the underlying directory, while its still mounted. If you use `ls` on a folder you wont see the mountpoint, but you can still access it.

**In version 3.3 of zfs-autobackup this will seem to be more prominent, because mounting has improved. Normally this problem would happen later after you reboot or zfs mount -a**

## Possible causes

Consider this pool:

```
dataset:         mounted at:
pool             /pool
pool/sub1        /pool/sub1
pool/sub1/sub2   /pool/sub1/sub2
```

This means the following mount-point directories where created:
* The root filesystem has a directory named `pool`
* Dataset `pool` has a directory named `sub1`
* Dataset `pool/sub1` has a directory named `sub2`

### Target side 

Now if we only select `pool` and `pool/sub1/sub2` to backup, then the target server will look like this:

```
dataset:                mounted at:
backup/pool             /pool
backup/pool/sub1/sub2   /pool/sub1/sub2
```

This means the following mount-point directories are needed:
* The root filesystem has a directory named `pool`
* Dataset `backup/pool` has a directory named `sub1/sub2` !

So `sub2` will be created in `sub1`, thus modifying the dataset!

## Solution

The best solution is to use the `--clear-mountpoint` option of zfs-autobackup. This will set canmount=noauto on newly received datasets.

When you need to access the data you can use zfs mount to mount just one dataset and leave the rest untouched.

**Note that this option also prevents bootproblems, in case a dataset has a mountpoint that conflicts with existing mountpoints.**

### Fix the problem manually

If you forgot to use --clear-mount, you can use some shell magic to fix it, for example for `test_target1`:

```console
 # zfs list -H -oname -r test_target1|xargs zfs set canmount=noauto
 # zfs list -H -oname -r test_target1|xargs -n1 zfs umount
```

After this you will have to use the `--rollback` option once to remove all the changes.

## Workarounds

These are some workarounds if cant use the method above, but they are not a perfect solution:

* Create the needed directories on the source side.
* Just use `--rollback` (or `--force`) all the time. This is kind of ugly and will confuse mountpoints and give various problems.
* Set the `readonly=on` property on the target dataset. This prevents modification, but doesn't allow you to mount some datasets.
* Manualy change the mountpoint of a target dataset to something like /mnt.

