from basetest import *
import time

class TestZfsAutobackup32(unittest2.TestCase):
    """various new 3.2 features"""

    def setUp(self):
        prepare_zpools()
        self.longMessage=True

    def test_invalid_common_snapshot(self):

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        #create 2 snapshots with the same name, which are invalid as common snapshot
        shelltest("zfs snapshot test_source1/fs1@invalid")
        shelltest("zfs snapshot test_target1/test_source1/fs1@invalid")

        with patch('time.strftime', return_value="test-20101111000001"):
            #try the old way (without guid checking), and fail:
            self.assertEqual(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --no-guid-check".split(" ")).run(),1)
            #new way should be ok:
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --no-snapshot".split(" ")).run())

            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000000
test_source1/fs1@invalid
test_source1/fs1@test-20101111000001
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000000
test_source1/fs1/sub@test-20101111000001
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000000
test_source2/fs2/sub@test-20101111000001
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1@invalid
test_target1/test_source1/fs1@test-20101111000001
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source1/fs1/sub@test-20101111000001
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
test_target1/test_source2/fs2/sub@test-20101111000001
""")

    def test_invalid_common_snapshot_with_data(self):

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        #create 2 snapshots with the same name, which are invalid as common snapshot
        shelltest("zfs snapshot test_source1/fs1@invalid")
        shelltest("touch /test_target1/test_source1/fs1/shouldnotbeHere")
        shelltest("zfs snapshot test_target1/test_source1/fs1@invalid")

        with patch('time.strftime', return_value="test-20101111000001"):
            #try the old way and fail:
            self.assertEqual(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --destroy-incompatible --no-guid-check".split(" ")).run(),1)
            #new way should be ok
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --no-snapshot --destroy-incompatible".split(" ")).run())

            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000000
test_source1/fs1@invalid
test_source1/fs1@test-20101111000001
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000000
test_source1/fs1/sub@test-20101111000001
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000000
test_source2/fs2/sub@test-20101111000001
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1@test-20101111000001
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source1/fs1/sub@test-20101111000001
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
test_target1/test_source2/fs2/sub@test-20101111000001
""")


    #check consistent mounting behaviour, see issue #112
    def test_mount_consitency_mounted(self):
        """only filesystems that have canmount=on with a mountpoint should be mounted. """

        shelltest("zfs create -V 10M test_source1/fs1/subvol")

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

            r=shelltest("zfs mount |grep -o /test_target1.*")
            self.assertMultiLineEqual(r,"""
/test_target1
/test_target1/test_source1/fs1
/test_target1/test_source1/fs1/sub
/test_target1/test_source2/fs2/sub
""")


    def test_mount_consitency_unmounted(self):
        """only test_target1 should be mounted in this test"""

        shelltest("zfs create -V 10M test_source1/fs1/subvol")

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --clear-mountpoint".split(" ")).run())

            r=shelltest("zfs mount |grep -o /test_target1.*")
            self.assertMultiLineEqual(r,"""
/test_target1
""")


    # def test_stuff(self):
    #
    #
    #     shelltest("zfs set autobackup:test=true test_source2")
    #     # shelltest("zfs set readonly=on test_target1")
    #
    #     with patch('time.strftime', return_value="test-20101111000000"):
    #         self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --clear-mountpoint".split(" ")).run())
    #
    #     # shelltest("zfs mount test_target1/test_source2/fs2/sub" )
    #
    #     with patch('time.strftime', return_value="test-20101111000001"):
    #         self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --rollback".split(" ")).run())


