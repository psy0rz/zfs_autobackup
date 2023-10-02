
from basetest import *


class TestZfsNode(unittest2.TestCase):

    def setUp(self):
        prepare_zpools()
        self.longMessage=True

    def test_keepsource0target10queuedsend(self):
        """Test if thinner doesnt destroy too much early on if there are no common snapshots YET. Issue #84"""

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose --keep-source=0 --keep-target=10 --allow-empty --no-send".split(
                    " ")).run())

        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose --keep-source=0 --keep-target=10 --allow-empty --no-send".split(
                    " ")).run())

        with mocktime("20101111000002"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose --keep-source=0 --keep-target=10 --allow-empty".split(
                    " ")).run())

        r = shelltest("zfs list -H -o name -r -t all " + TEST_POOLS)
        self.assertMultiLineEqual(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000002
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000002
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000002
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1@test-20101111000001
test_target1/test_source1/fs1@test-20101111000002
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source1/fs1/sub@test-20101111000001
test_target1/test_source1/fs1/sub@test-20101111000002
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
test_target1/test_source2/fs2/sub@test-20101111000001
test_target1/test_source2/fs2/sub@test-20101111000002
""")


    def test_excludepaths(self):
        """Test issue #103"""

        shelltest("zfs create test_target1/target_shouldnotbeexcluded")
        shelltest("zfs set autobackup:test=true test_target1/target_shouldnotbeexcluded")
        shelltest("zfs create test_target1/target")

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1/target --no-progress --verbose --allow-empty".split(
                    " ")).run())


        r = shelltest("zfs list -H -o name -r -t all " + TEST_POOLS)
        self.assertMultiLineEqual(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000000
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000000
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000000
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/target
test_target1/target/test_source1
test_target1/target/test_source1/fs1
test_target1/target/test_source1/fs1@test-20101111000000
test_target1/target/test_source1/fs1/sub
test_target1/target/test_source1/fs1/sub@test-20101111000000
test_target1/target/test_source2
test_target1/target/test_source2/fs2
test_target1/target/test_source2/fs2/sub
test_target1/target/test_source2/fs2/sub@test-20101111000000
test_target1/target/test_target1
test_target1/target/test_target1/target_shouldnotbeexcluded
test_target1/target/test_target1/target_shouldnotbeexcluded@test-20101111000000
test_target1/target_shouldnotbeexcluded
test_target1/target_shouldnotbeexcluded@test-20101111000000
""")


