## Performance tips

Depending on your situation, these performance tips may help a lot.

## Speeding up SSH

You can make your ssh connections persistent and greatly speed up zfs-autobackup:

On the server that initiates the backup add this to your ~/.ssh/config:

```console
Host *
    ControlPath ~/.ssh/control-master-%r@%h:%p
    ControlMaster auto
    ControlPersist 3600
```

Thanks @mariusvw :)

### Direct TCP network transfer

If ssh-encryption performance is still too slow for your use-case, you can do a direct unencrypted transfer via tcp ip:

Use something like this:
```console
zfs-autobackup ... --send-pipe "nc server_name 8023" --recv-pipe "nc -l -p 8023"
```

This will pipe the data through netcat on the specified port. (You can use any transfer program you want this way)

Note that only the actual transfer of the ZFS-data during zfs send/recv is done via this, it still requires SSH for all the other stuff.

Also see: https://github.com/psy0rz/zfs_autobackup/issues/15#issuecomment-1043753454

## Buffering and compression

Also it might help to use the `--buffer` option to use IO buffering during the data transfer. 

This might speed up things since it smooths out sudden IO bursts that are frequent during a zfs send or recv.

If you have a slow link and compressible data, look at the `--compress` option.

Also see [[Piping]]

## Less zfs commands

You can make zfs-autobackup use less commands and IO per snapshot transfer by:

* `--no-holds`: to prevent the hold/release commands.
* `--allow-empty`: to prevent commands to figure out if a snapshot would be empty.
* `--no-guid-check`: dont check if common snapshots have the same guid.
* `--clear-mountpoint`: not mounting the dataset on the target saves time.

## Disable progress (ZFS bug)

There is actually a performance regression in ZFS version 2: https://github.com/openzfs/zfs/issues/11560 

This bug will delay each transfer by 1 second. This is a problem if you have lots of small transfers.

Use --no-progress as workaround.

## Some statistics

To get some idea of how fast zfs-autobackup is, I did some test on my laptop, with a SKHynix_HFS512GD9TNI-L2B0B disk. I'm using zfs 2.0.2.  

I created 100 empty datasets and measured the total runtime of zfs-autobackup. I used --no-holds, --allow-empty and ssh ControlMaster.

* without ssh: 15 seconds. (>6 datasets/s)
* either ssh-target or ssh-source=localhost: 20 seconds (5 datasets/s)
* both ssh-target and ssh-source=localhost: 24 seconds (4 datasets/s)

To be bold I created 2500 datasets, but that also was no problem. So it seems it should be possible to use zfs-autobackup with thousands of datasets.

If you need more performance let me know.

## TCP tuning

By default, the linux kernel uses `cubic` as the TCP Congestion Control protocol. This is extremely good for _almost_ all situations, except for moving huge amounts of data over the public internet where you can have brief bursts of congestion (and therefore packet loss).

Changing this to `scaleable` makes TCP sessions recover much faster from packet loss, and return to the full speed available without the extended recovery of the normal `cubic` protocol.

If you're sending data over the internet, this setting might help:

/etc/sysctl.d/60-send-bulk-tcp.conf:
```
# Recovers rapidly from congestion
net.ipv4.tcp_congestion_control = scalable
```

More info: https://en.wikipedia.org/wiki/Scalable_TCP

(thanks @xrobau)

