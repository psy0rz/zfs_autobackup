
# To run tests as non-root, use this hack:
# chmod 4755 /usr/sbin/zpool /usr/sbin/zfs

import subprocess
import random

#default test stuff
import unittest2
import subprocess
import time
from pprint import *
from zfs_autobackup.ZfsAutobackup import *
from zfs_autobackup.ZfsAutoverify import *
from zfs_autobackup.ZfsCheck import *
from zfs_autobackup.util import *
from mock import *
import contextlib
import sys
import io

TEST_POOLS="test_source1 test_source2 test_target1"
ZFS_USERSPACE=  subprocess.check_output("dpkg-query -W zfsutils-linux |cut -f2", shell=True).decode('utf-8').rstrip()
ZFS_KERNEL=     subprocess.check_output("modinfo zfs|grep ^version |sed 's/.* //'", shell=True).decode('utf-8').rstrip()

print("###########################################")
print("#### Unit testing against:")
print("#### Python                :"+sys.version.replace("\n", " "))
print("#### ZFS userspace         :"+ZFS_USERSPACE)
print("#### ZFS kernel            :"+ZFS_KERNEL)
print("#############################################")



# for python2 compatibility
if sys.version_info.major==2:
    OutputIO=io.BytesIO
else:
    OutputIO=io.StringIO


# for python2 compatibility (python 3 has this already)
@contextlib.contextmanager
def redirect_stdout(target):
    original = sys.stdout
    try:
        sys.stdout = target
        yield
    finally:
        sys.stdout = original

# for python2 compatibility (python 3 has this already)
@contextlib.contextmanager
def redirect_stderr(target):
    original = sys.stderr
    try:
        sys.stderr = target
        yield
    finally:
        sys.stderr = original



def shelltest(cmd):
    """execute and print result as nice copypastable string for unit tests (adds extra newlines on top/bottom)"""

    ret=(subprocess.check_output("SUDO_ASKPASS=./password.sh sudo -A "+cmd , shell=True).decode('utf-8'))

    print("######### result of: {}".format(cmd))
    print(ret)
    print("#########")
    ret='\n'+ret
    return(ret)

def prepare_zpools():
    print("Preparing zfs filesystems...")

    #need ram blockdevice
    subprocess.check_call("modprobe brd rd_size=512000", shell=True)

    #remove old stuff
    subprocess.call("zpool destroy test_source1 2>/dev/null", shell=True)
    subprocess.call("zpool destroy test_source2 2>/dev/null", shell=True)
    subprocess.call("zpool destroy test_target1 2>/dev/null", shell=True)

    #create pools
    subprocess.check_call("zpool create test_source1 /dev/ram0", shell=True)
    subprocess.check_call("zpool create test_source2 /dev/ram1", shell=True)
    subprocess.check_call("zpool create test_target1 /dev/ram2", shell=True)

    #create test structure
    subprocess.check_call("zfs create -p test_source1/fs1/sub", shell=True)
    subprocess.check_call("zfs create -p test_source2/fs2/sub", shell=True)
    subprocess.check_call("zfs create -p test_source2/fs3/sub", shell=True)
    subprocess.check_call("zfs set autobackup:test=true test_source1/fs1", shell=True)
    subprocess.check_call("zfs set autobackup:test=child test_source2/fs2", shell=True)

    print("Prepare done")
