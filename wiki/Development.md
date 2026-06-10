These are instructions if you want to modify zfs-autobackup, or if you want to run zfs-autobackup directly from git.

## Setting up virtual env

(this is optional, you can also install the requirements globally)

```console
psy@ws1:~/zfs_autobackup$ python3 -m venv backupdev
psy@ws1:~/zfs_autobackup$ source backupdev/bin/activate
(backupdev) psy@ws1:~/zfs_autobackup$ pip install -r requirements.txt 
psy@ws1 ~/zfs_autobackup % python3 -m venv backupdev                 
...
```

## Running zfs_autobackup

To run zfs_autobackup you can just access it as a python module:

```console
(backupdev) psy@ws1:~/zfs_autobackup$ python -m zfs_autobackup.ZfsAutobackup  --version
ZfsAutobackup.py v3.2-alpha1 - (c)2021 E.H.Eefting (edwin@datux.nl)
```

Same goes for zfs-autoverify:
```console
(backupdev) psy@ws1:~/zfs_autobackup$ python -m zfs_autobackup.ZfsAutoverify  --version
ZfsAutoverify.py v3.2-alpha1 - (c)2021 E.H.Eefting (edwin@datux.nl)
```

## Automated test suites

I'm using a test driven design process, usually i write the code and corresponding tests in unison.

## Running tests via docker

This is the easiest way to run the testsuite against the zfs modules in the running kernel. It will use Alpine for the zfs userspace utilities.

From the main repo just run: `./tests/run_tests_docker`

You can use the unittest -k option to run only specific tests: `./tests/autorun_tests_docker -k test_thinner.TestThinner`


## Running directly (without docker)

This can be a bit more tricky.

The tests run against actual zfs commands and create a lot of temporary pools via loopback-images in the /tmp dir. Therefore the tests need root to run.

The tests also need ssh support via root@localhost, so it will create and install a sshkey if needed.

### Running the whole suite

This takes a few minutes and is also done automaticly on each commit via github actions: https://github.com/psy0rz/zfs_autobackup/actions 

```console
(backupdev) root@ws1:/home/psy/zfs_autobackup# ./tests/run_tests 
###########################################
#### Unit testing against:
#### Python                :3.8.10 (default, Nov 26 2021, 20:14:08)  [GCC 9.3.0]
#### ZFS userspace         :2.1.1-0york0~20.04
#### ZFS kernel            :2.1.1-0york0~20.04
#############################################
THIS TEST REQUIRES SSH TO LOCALHOST
test_exitcode (test_cmdpipe.TestCmdPipe)
test piped exitcodes ... ok
...
```

### Running one test

Since running the whole suite takes long, you can run one test like this:

```console
(backupdev) root@ws1:/home/psy/zfs_autobackup# ./tests/run_test test_verify.py
###########################################
#### Unit testing against:
#### Python                :3.8.10 (default, Nov 26 2021, 20:14:08)  [GCC 9.3.0]
#### ZFS userspace         :2.1.1-0york0~20.04
#### ZFS kernel            :2.1.1-0york0~20.04
#############################################
test_verify (test_verify.TestZfsEncryption) ... Preparing zfs filesystems...
```

## Running directly from pycharm

### Method 1, remote via ssh:

This requires pycharm professional, but makes things a lot easier: Tests are just uploaded and runned via ssh on a remote linux server.

This is also nice if you're developing in macos or windows.

Just add an ssh interpreter to a remote server that has zfs support. 

### Method 2, locally:

You need a trick to run all the stuff as root, but still having the advantage of running the tests from the editor.

Just create a venv as you normally would, but then replace the python symlink with a copy of the binary and do this:
```
user@host:~/zfs_autobackup/venv/bin$ sudo chown root:root python
user@host:~/zfs_autobackup/venv/bin$ sudo chmod 4755 python
```

This has obvious security implications of course, but so far this seems to be the best way to do it. 


