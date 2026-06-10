## Backup a proxmox cluster with HA replication

We've created a script that will backup all the relevant data of your whole proxmox cluster. (your cluster has to be installed with zfs)

Proxmox can do zfs replication between nodes if you enable it. Because of this, a dataset can exist on multiple nodes, but is only active at one of the nodes. We need to make sure we only backup the active dataset and exclude the rest. This is where the --exclude-unchanged option comes in.

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

Remember, if you need specific ssh-settings, use ~/.ssh/config. 

```shell
#!/bin/bash

#v1.1: use  --exclude-unchanged instead of --ignore-replicated

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
  zfs-autobackup --keep-source=1d1w,1w1m --ssh-source $HOST data_$NAME $TARGET/data --clear-mountpoint --clear-refreservation --ignore-transfer-errors --strip-path 2 --verbose   --exclude-unchanged 300000 --no-holds   $@

  zabbix-job-status backup_$HOST""_data_$NAME daily $? >/dev/null 2>/dev/null

done
```

This script will also send the backup status to Zabbix. (if you've installed my zabbix-job-status script https://github.com/psy0rz/stuff/tree/master/zabbix-jobs)
