
from basetest import *


# test zfs-verify:
# - when there is no common snapshot at all
# - when encryption key not loaded
# - test mode
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

        shelltest("zfs create test_source1/fs1/ok_filesystem")
        shelltest("cp *.py /test_source1/fs1/ok_filesystem")
        shelltest("zfs create -V 1M test_source1/fs1/ok_zvol")
        shelltest("dd if=/dev/urandom of=/dev/zvol/test_source1/fs1/ok_zvol count=1 bs=512k")

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-progress".split(" ")).run())

        # make sure we cant accidently compare current data
        shelltest("rm /test_source1/fs1/ok_filesystem/*")
        shelltest("dd if=/dev/zero of=/dev/zvol/test_source1/fs1/ok_zvol count=1 bs=512k")

    def test_verify(self):

        self.assertFalse(ZfsAutoverify("test test_target1 --verbose --test".split(" ")).run())

        self.assertFalse(ZfsAutoverify("test test_target1 --verbose".split(" ")).run())

