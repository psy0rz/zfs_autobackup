## Thinning out obsolete snapshots

The thinner is the thing that destroys old snapshots on the source and target.

The thinner operates "stateless": There is nothing in the name or properties of a snapshot that indicates how long it will be kept. Everytime zfs-autobackup runs, it will look at the timestamp of all the existing snapshots. From there it will determine which snapshots are obsolete according to your schedule. The advantage of this stateless system is that you can always change the schedule.

Another advantage is if your backup has failed for a longer time: It might have "missed" your monthly backup date, but the thinner then still holds the snapshot thats closest to the intended target date. 

## Which snapshots is it operating on?

A snapshot name should match `--snapshot-format` to be considered for deletion. 

This means that:
* On the source, only snapshots on datasets that are **selected** are considered.
* On the target, **all** snapshots are considered recursively. 

## Thinning schedule

The thinning schedule is specified via the `--keep-source=...` and `--keep-target=...` parameters.

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
[root@backup ~]# zfs-autobackup -v offsite1 --keep-source=10,1d1w,1w1m,1m1y
  zfs-autobackup v3.1.1 - (c)2021 E.H.Eefting (edwin@datux.nl)
  
  Selecting dataset property : autobackup:offsite1
  Snapshot format            : offsite1-%Y%m%d%H%M%S
  Hold name                  : zfs_autobackup:offsite1
  
  #### Source settings
  [Source] Datasets are local
  [Source] Keep the last 10 snapshots.
  [Source] Keep every 1 day, delete after 1 week.
  [Source] Keep every 1 week, delete after 1 month.
  [Source] Keep every 1 month, delete after 1 year.
...
```

A snapshot will only be destroyed if it not needed anymore by ANY of the rules.

You can specify as many rules as you need. The order of the rules doesn't matter.

Keep in mind its up to you to actually run zfs-autobackup often enough: If you want to keep hourly snapshots, you have to make sure you at least run it every hour.

However, its no problem if you run it more or less often than that: The thinner will still keep an optimal set of snapshots to match your schedule as close as possible.

If you want to keep as few snapshots as possible, just specify 0. (`--keep-source=0` for example)

Since version 4.0 a `--keep-source=0` really keeps nothing on the source: the common snapshot is replaced by a [bookmark](Common-snapshots-and-holds), which takes up almost no space. (In older versions the common snapshot was always kept) It also automatically implies `--min-change=0`, since there is no snapshot left to compare changes against.

If you want to keep ALL the snapshots, just specify a high number.

## Destroying missing datasets

When a dataset has been destroyed or deselected on the source, but still exists on the target we call it a missing dataset. Missing datasets will be still thinned out according to the schedule. (Unless `--no-thinning` is used)

The final snapshot will never be destroyed, unless you specify a **deadline** with the `--destroy-missing` option:

In that case it will look at the last snapshot we took and determine if is older than the deadline you specified. e.g: `--destroy-missing 30d` will start destroying things 30 days after the last snapshot.

### After the deadline

When the deadline is passed, all our snapshots, except the last one will be destroyed. Irregardless of the normal thinning schedule.

The dataset has to have the following properties to be finally really destroyed:

* The dataset has no direct child-filesystems or volumes.
* The only snapshot left is the last one created by zfs-autobackup.
* The remaining snapshot has no clones.

## Technical details about the Thinner

Only read this section if you want to exactly know whats going on.

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

