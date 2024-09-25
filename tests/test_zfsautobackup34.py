from basetest import *

class TestZfsAutobackup32(unittest2.TestCase):
    """various new 3.4 features"""

    def setUp(self):
        prepare_zpools()
        self.longMessage=True

    def test_select_bookmark_or_snapshot(self):
        """test if zfs autobackup chooses the most recent common matching dataset when there are both bookmarks and snapshots, some with the wrong GUID"""

        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --no-holds".split(" ")).run())


        shelltest("zfs destroy test_source2/fs2/sub@test-20101111000001")
        shelltest("zfs destroy test_source1/fs1/sub#test-20101111000001")

        #bookmark with incorrect GUID, should fallback to snapshot
        shelltest("zfs destroy test_source1/fs1#test-20101111000001")
        shelltest("zfs snapshot test_source1/fs1@wrong")
        shelltest("zfs bookmark test_source1/fs1@wrong \#test-20101111000001")
        shelltest("zfs destroy test_source1/fs1@wrong")

        with mocktime("20101111000002"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --no-holds".split(" ")).run())


        r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
        self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000001
test_source1/fs1@test-20101111000002
test_source1/fs1#test-20101111000001
test_source1/fs1#test-20101111000002
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000001
test_source1/fs1/sub@test-20101111000002
test_source1/fs1/sub#test-20101111000002
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000002
test_source2/fs2/sub#test-20101111000002
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000001
test_target1/test_source1/fs1@test-20101111000002
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000001
test_target1/test_source1/fs1/sub@test-20101111000002
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000001
test_target1/test_source2/fs2/sub@test-20101111000002
""")

