
from basetest import *


# test zfs-verify:
# - when there is no common snapshot at all
# - when encryption key not loaded
# - --test mode
# - --fs-compare methods
# - on snapshots of datasets:
#   - that are correct
#   - that are different
#   - because of rsync: test local/local, local remote etc
# - on snapshots of zvols
#  - that are correct
#  - that are different
#

class TestZfsEncryption(unittest2.TestCase):


    def setUp(self):
        prepare_zpools()

        #create actual test files and data
        shelltest("zfs create test_source1/fs1/ok_filesystem")
        shelltest("cp tests/*.py /test_source1/fs1/ok_filesystem")

        shelltest("zfs create test_source1/fs1/bad_filesystem")
        shelltest("cp tests/*.py /test_source1/fs1/bad_filesystem")

        shelltest("zfs create -V 1M test_source1/fs1/ok_zvol")
        shelltest("dd if=/dev/urandom of=/dev/zvol/test_source1/fs1/ok_zvol count=1 bs=512k")


        #create backup
        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-progress --no-holds".split(" ")).run())

        #Do an ugly hack to create a fault in the bad filesystem
        #In zfs-autoverify it doenst matter that the snapshot isnt actually the same snapshot, so this hack works
        shelltest("zfs destroy test_target1/test_source1/fs1/bad_filesystem@test-20101111000000")
        shelltest("zfs mount test_target1/test_source1/fs1/bad_filesystem")
        shelltest("echo >> /test_target1/test_source1/fs1/bad_filesystem/test_verify.py")
        shelltest("zfs snapshot test_target1/test_source1/fs1/bad_filesystem@test-20101111000000")

        # make sure we cant accidently compare current data
        shelltest("zfs mount test_target1/test_source1/fs1/ok_filesystem")
        shelltest("rm /test_source1/fs1/ok_filesystem/*")
        # shelltest("zfs mount /test_target1/test_source1/fs1/bad_filesystem")
        shelltest("rm /test_source1/fs1/bad_filesystem/*")
        shelltest("dd if=/dev/zero of=/dev/zvol/test_source1/fs1/ok_zvol count=1 bs=512k")

    def test_verify(self):

        self.assertFalse(ZfsAutoverify("test test_target1 --verbose --test".split(" ")).run())

        #rsync mode
        self.assertEqual(1, ZfsAutoverify("test test_target1 --verbose".split(" ")).run())
        self.assertEqual(1, ZfsAutoverify("test test_target1 --ssh-source=localhost --verbose --exclude-received".split(" ")).run())
        self.assertEqual(1, ZfsAutoverify("test test_target1 --ssh-target=localhost --verbose --exclude-received".split(" ")).run())

