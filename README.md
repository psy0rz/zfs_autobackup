# ZFS autobackup

Introduction
============

ZFS autobackup is used to periodicly backup ZFS filesystems to other locations. This is done using the very effcient zfs send and receive commands.

It has the following features:
* Automaticly selects filesystems to backup by looking at a simple ZFS property.
* 
