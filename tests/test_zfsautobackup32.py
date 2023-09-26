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
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

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
        shelltest("zfs mount test_target1/test_source1/fs1")
        shelltest("touch /test_target1/test_source1/fs1/shouldnotbeHere")
        shelltest("zfs snapshot test_target1/test_source1/fs1@invalid")

        with patch('time.strftime', return_value="test-20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --destroy-incompatible".split(" ")).run())

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

