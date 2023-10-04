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




    def test_transfer_thinning(self):
        # test pre/post/during transfer thinning and efficient transfer (no transerring of stuff that gets deleted on target)

        #less output
        shelltest("zfs set autobackup:test2=true test_source1/fs1/sub")

        # nobody wants this one, will be destroyed before transferring (over a year ago)
        with mocktime("20000101000000"):
            self.assertFalse(ZfsAutobackup("test2 --allow-empty".split(" ")).run())

        # only target wants this one (monthlys)
        with mocktime("20010101000000"):
            self.assertFalse(ZfsAutobackup("test2 --allow-empty".split(" ")).run())

        # both want this one (dayly + monthly)
        # other snapshots should influence the middle one that we actually want.
        with mocktime("20010201000000"):
            shelltest("zfs snapshot test_source1/fs1/sub@other1")
            self.assertFalse(ZfsAutobackup("test2 --allow-empty".split(" ")).run())
            shelltest("zfs snapshot test_source1/fs1/sub@other2")

        # only source wants this one (dayly)
        with mocktime("20010202000000"):
            self.assertFalse(ZfsAutobackup("test2 --allow-empty".split(" ")).run())

        #will become common snapshot
        with OutputIO() as buf:
            with redirect_stdout(buf):
                with mocktime("20010203000000"):
                    self.assertFalse(ZfsAutobackup("--keep-source=1d10d --keep-target=1m10m --allow-empty --verbose --clear-mountpoint --other-snapshots test2 test_target1".split(" ")).run())


            print(buf.getvalue())
            self.assertIn(
"""
  [Source] test_source1/fs1/sub@test2-20000101000000: Destroying
  [Source] test_source1/fs1/sub@test2-20010101000000: -> test_target1/test_source1/fs1/sub (new)
  [Source] test_source1/fs1/sub@other1: -> test_target1/test_source1/fs1/sub
  [Source] test_source1/fs1/sub@test2-20010101000000: Destroying
  [Source] test_source1/fs1/sub@test2-20010201000000: -> test_target1/test_source1/fs1/sub
  [Source] test_source1/fs1/sub@other2: -> test_target1/test_source1/fs1/sub
  [Source] test_source1/fs1/sub@test2-20010203000000: -> test_target1/test_source1/fs1/sub
""", buf.getvalue())


        r=shelltest("zfs list -H -o name -r -t snapshot test_source1 test_target1")
        self.assertMultiLineEqual(r,"""
test_source1/fs1/sub@other1
test_source1/fs1/sub@test2-20010201000000
test_source1/fs1/sub@other2
test_source1/fs1/sub@test2-20010202000000
test_source1/fs1/sub@test2-20010203000000
test_target1/test_source1/fs1/sub@test2-20010101000000
test_target1/test_source1/fs1/sub@other1
test_target1/test_source1/fs1/sub@test2-20010201000000
test_target1/test_source1/fs1/sub@other2
test_target1/test_source1/fs1/sub@test2-20010203000000
""")


