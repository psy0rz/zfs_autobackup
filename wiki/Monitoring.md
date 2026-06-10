## Monitoring if zfs-autobackup succeeds

On completion, zfs-autobackup returns an exit code:

* `0`: Everything went fine, no important errors.
* `x`: Snapshotting was ok, but there are `x` datasets that aborted with a fatal error.
* `255`: Wrong options or other major failure, probably nothing succeeded. 

### Output

Without `--verbose` or `--debug`, zfs-autobackup only echos (zfs) errors and warnings to stderr. Complete silence means everything is fine.

So if you stick it in a crontab it should only mail you if something is wrong.

If it detects a tty it will output progress updates to stdout.

## Monitoring example with Zabbix-jobs

You can monitor backups by using my zabbix-jobs script. (<https://github.com/psy0rz/stuff/tree/master/zabbix-jobs>)

Put this command directly after the zfs_backup command in your cronjob:

```console
zabbix-job-status backup_smartos01_fs1 daily $?
```

This will update the zabbix server with the exit code and will also alert you if the job didn't run for more than 2 days.
