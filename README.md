# ZFS autobackup

Introduction
============

ZFS autobackup is used to periodicly backup ZFS filesystems to other locations. This is done using the very effcient zfs send and receive commands.

It has the following features:
* Automaticly selects filesystems to backup by looking at a simple ZFS property.
* Is able to "push" and "pull" the backups via SSH.
* Also supports local backups.
* Even supports pulling data from a source-host and pushing backup to target host by ssh.
* Can be scheduled via a simple cronjob orrun directly from commandline.
* Always creates new snapshots, even if the previous backup was aborted.
* Ability to 'finish' aborted backups to see what goes wrong.
* Checks everything and aborts on errors.
* Written in python and uses zfs-commands, no 3rd party dependencys or libraries.
* Easy to debug and has a test-mode. Actual unix commands are printed.

Usage
=====



