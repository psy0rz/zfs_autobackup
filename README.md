
# ZFS autobackup

[![Tests](https://github.com/psy0rz/zfs_autobackup/workflows/Regression%20tests/badge.svg)](https://github.com/psy0rz/zfs_autobackup/actions?query=workflow%3A%22Regression+tests%22) [![Coverage Status](https://coveralls.io/repos/github/psy0rz/zfs_autobackup/badge.svg)](https://coveralls.io/github/psy0rz/zfs_autobackup)  [![Python Package](https://github.com/psy0rz/zfs_autobackup/workflows/Upload%20Python%20Package/badge.svg)](https://pypi.org/project/zfs-autobackup/)

## Introduction

ZFS-autobackup tries to be the most reliable and easiest to use tool, while having all the features.

You can either use it as a **backup** tool, **replication** tool or **snapshot** tool.

You can select what to backup by setting a custom `ZFS property`. This makes it easy to add/remove specific datasets, or just backup your whole pool.

Other settings are just specified on the commandline: Simply setup and test your zfs-autobackup command and  fix all the issues you might encounter. When you're done you can just copy/paste your command to a cron or script.

Since it's using ZFS commands, you can see what it's actually doing by specifying `--debug`. This also helps a lot if you run into some strange problem or error. You can just copy-paste the command that fails and play around with it on the commandline. (something I missed in other tools)

An important feature that's missing from other tools is a reliable `--test` option: This allows you to see what zfs-autobackup will do and tune your parameters. It will do everything, except make changes to your system.

## Features

* Works across operating systems: Tested with **Linux**, **FreeBSD/FreeNAS** and **SmartOS**.
* Low learning curve: no complex daemons or services, no additional software or networking needed. (Only read this page)   
* Plays nicely with existing replication systems. (Like Proxmox HA)
* Automatically selects filesystems to backup by looking at a simple ZFS property. 
* Creates consistent snapshots. (takes all snapshots at once, atomicly.)
* Multiple backups modes:
  * Backup local data on the same server.
  * "push" local data to a backup-server via SSH.
  * "pull" remote data from a server via SSH and backup it locally.
  * "pull+push": Zero trust between source and target.
* Can be scheduled via simple cronjob or run directly from commandline.
* ZFS encryption support: Can decrypt / encrypt or even re-encrypt datasets during transfer.
* Supports sending with compression. (Using pigz, zstd etc)
* IO buffering to speed up transfer.
* Bandwidth rate limiting.
* Multiple backups from and to the same datasets are no problem.
* Resillient to errors.
* Ability to manually 'finish' failed backups to see whats going on.
* Easy to debug and has a test-mode. Actual unix commands are printed.
* Uses progressive thinning for older snapshots.
* Uses zfs-holds on important snapshots to prevent accidental deletion.
* Automatic resuming of failed transfers.
* Easy migration from existing zfs backups.
* Gracefully handles datasets that no longer exist on source.
* Complete and clean logging. 
* Easy installation:
  * Just install zfs-autobackup via pip.
  * Only needs to be installed on one side.
  * Written in python and uses zfs-commands, no special 3rd party dependency's or compiled libraries needed.
  * No annoying config files or properties. 

## Getting started

Please look at our wiki to [Get started](https://github.com/psy0rz/zfs_autobackup/wiki).

# Sponsor list

This project was sponsorred by:

* JetBrains (Provided me with a license for their whole professional product line, https://www.jetbrains.com/pycharm/ )
