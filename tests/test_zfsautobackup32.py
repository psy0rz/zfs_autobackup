from basetest import *

class TestZfsAutobackup32(unittest2.TestCase):
    """various new 3.2 features"""

    def setUp(self):
        prepare_zpools()
        self.longMessage=True

    def test_invalid_common_snapshot(self):

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        #create 2 snapshots with the same name, which are invalid as common snapshot
        shelltest("zfs snapshot test_source1/fs1@invalid")
        shelltest("zfs snapshot test_target1/test_source1/fs1@invalid")

        with mocktime("20101111000001"):
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

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        #create 2 snapshots with the same name, which are invalid as common snapshot
        shelltest("zfs snapshot test_source1/fs1@invalid")
        shelltest("touch /test_target1/test_source1/fs1/shouldnotbeHere")
        shelltest("zfs snapshot test_target1/test_source1/fs1@invalid")

        with mocktime("20101111000001"):
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

        with mocktime("20101111000000"):
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

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --clear-mountpoint".split(" ")).run())

            r=shelltest("zfs mount |grep -o /test_target1.*")
            self.assertMultiLineEqual(r,"""
/test_target1
""")




    #XXX: VERBERTERING VAN ADD VIRTUALSNAPSHOTS IN GIT STASH!
    def test_thinning(self):

        # time_str = "20111112000000"  # month in the "future"
        # future_timestamp = time_secs = time.mktime(time.strptime(time_str, "%Y%m%d%H%M%S"))
        # with patch('time.time', return_value=future_timestamp):

        with  mocktime("20001001000000"):
            print(datetime_now(False))
            self.assertFalse(ZfsAutobackup("test --allow-empty --clear-mountpoint --verbose".split(" ")).run())

#         with mocktime("20001101000000"):
#             self.assertFalse(ZfsAutobackup("test --allow-empty --clear-mountpoint test_target1 --no-progress --allow-empty --clear-mountpoint".split(" ")).run())
#
#         with mocktime("20001201000000"):
#             self.assertFalse(ZfsAutobackup("test --allow-empty --clear-mountpoint".split(" ")).run())
#
#         with mocktime("20001202000000"):
#             self.assertFalse(ZfsAutobackup("test --allow-empty --clear-mountpoint".split(" ")).run())
#
#         time_str="test-20001203000000"
#         with patch('time.time', return_value=time.mktime(time.strptime(time_str, "test-%Y%m%d%H%M%S"))):
#             with patch('time.strftime', return_value=time_str):
#                 self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --allow-empty --clear-mountpoint --keep-source=1d2d".split(" ")).run())
#
#
#
#             r=shelltest("zfs list -H -o name -r -t snapshot test_source1 test_target1")
#             self.assertMultiLineEqual(r,"""
# /test_target1
# /test_target1/test_source1/fs1
# /test_target1/test_source1/fs1/sub
# /test_target1/test_source2/fs2/sub
# """)
