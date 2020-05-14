import subprocess
import random

#default test stuff
import unittest
import subprocess
import time
from pprint import *
from bin.zfs_autobackup import *


def prepare_zpools():
    print("Preparing zfs filesystems...")

    #need ram blockdevice
    # subprocess.call("rmmod brd", shell=True)
    subprocess.check_call("modprobe brd rd_size=512000", shell=True)

    #remove old stuff
    subprocess.call("zpool destroy test_source1", shell=True)
    subprocess.call("zpool destroy test_source2", shell=True)
    subprocess.call("zpool destroy test_target1", shell=True)

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
