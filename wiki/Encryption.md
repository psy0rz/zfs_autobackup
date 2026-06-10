## How does zfs-autobackup handle encryption?

In normal operation, datasets are transferred unaltered:

* Source datasets that are encrypted will be send over as such and stay encrypted at the target side. (In ZFS this is called raw-mode) You dont need keys at the target side if you dont need to access the data. This is especially usefull if you backup to untrusted servers.
* Source datasets that are plain will stay that way on the target. (Even if the specified target-path IS encrypted.)

Basically you dont have to do anything or worry about anything.

## Decrypting/encrypting

Things get more interesting if you want to change the encryption-state of a dataset during transfer:

* If you want to decrypt encrypted datasets before sending them, you should use the `--decrypt` option. Datasets will then be stored plain at the target.
* If you want to encrypt plain datasets when they are received, you should use the `--encrypt` option. Datasets will then be stored encrypted at the target. Datasets that are already encrypted will still be sent over unaltered in raw-mode.
* If you also want re-encrypt encrypted datasets with the target-side encryption you can use both options. 


## Notes

* The --encrypt option will rely on inheriting encryption parameters from the parent datasets on the target side. You are responsible for setting those up and loading the keys. So --encrypt is no guarantee for encryption: If you dont set it up, it cant encrypt. (and will store the data unencrypted)

* When using --decrypt, dataset properties will NOT be sent over, due to a bug in ZFS itself: https://github.com/openzfs/zfs/issues/16275 (zfs-autobackup will warn you about this)

* --encrypt will be ignored for datasets that are already encrypted: These are transferred in raw mode, unless you specify --decrypt as well.

* Decide what you want at an early stage: If you change the --encrypt or --decrypt parameter after the inital sync you might get weird and wonderfull errors. (nothing dangerous)

## Some common errors while using zfs encryption

> cannot receive incremental stream: kernel modules must be upgraded to receive this stream.

This happens if you forget to use `--encrypt`, while the target datasets are already encrypted. A very strange error message indeed.
