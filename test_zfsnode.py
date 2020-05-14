
#default test stuff
import unittest
from bin.zfs_autobackup import *

import subprocess
import time
from pprint import pformat

class TestZfsNode(unittest.TestCase):

    def setUp(self):
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

        return super().setUp()



    def test_getselected(self):
        logger=Logger()
        description="[Source]"
        node=ZfsNode("test", logger, description=description)
        s=pformat(node.selected_datasets)
        print(s)

        #basics
        self.assertEqual (s, """[(local): test_source1/fs1,
 (local): test_source1/fs1/sub,
 (local): test_source2/fs2/sub]""")

        #caching, so expect same result
        subprocess.check_call("zfs set autobackup:test=true test_source2/fs3", shell=True)
        self.assertEqual (s, """[(local): test_source1/fs1,
 (local): test_source1/fs1/sub,
 (local): test_source2/fs2/sub]""")



if __name__ == '__main__':
    unittest.main()