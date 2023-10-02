from basetest import *
import time

class TestZfsAutobackup31(unittest2.TestCase):
    """various new 3.1 features"""

    def setUp(self):
        prepare_zpools()
        self.longMessage=True

    def test_no_thinning(self):

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --keep-target=0 --keep-source=0 --no-thinning".split(" ")).run())

            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            self.assertMultiLineEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000000
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


    def test_re_replication(self):
        """test re-replication of something thats already a backup (new in v3.1-beta5)"""

        shelltest("zfs create test_target1/a")
        shelltest("zfs create test_target1/b")

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1/a --no-progress --verbose --debug".split(" ")).run())

        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1/b --no-progress --verbose".split(" ")).run())

            r=shelltest("zfs list -H -o name -r -t snapshot test_target1")
            #NOTE: it wont backup test_target1/a/test_source2/fs2/sub to test_target1/b since it doesnt have the zfs_autobackup property anymore.
            self.assertMultiLineEqual(r,"""
test_target1/a/test_source1/fs1@test-20101111000000
test_target1/a/test_source1/fs1/sub@test-20101111000000
test_target1/a/test_source2/fs2/sub@test-20101111000000
test_target1/b/test_source1/fs1@test-20101111000000
test_target1/b/test_source1/fs1/sub@test-20101111000000
test_target1/b/test_source2/fs2/sub@test-20101111000000
test_target1/b/test_target1/a/test_source1/fs1@test-20101111000000
test_target1/b/test_target1/a/test_source1/fs1/sub@test-20101111000000
""")

    def test_zfs_compressed(self):

        with mocktime("20101111000000"):
            self.assertFalse(
                ZfsAutobackup("test test_target1 --no-progress --verbose --debug --zfs-compressed".split(" ")).run())

    def test_force(self):
        """test 1:1 replication"""

        shelltest("zfs set autobackup:test=true test_source1")

        with mocktime("20101111000000"):
            self.assertFalse(
                ZfsAutobackup("test test_target1 --no-progress --verbose --debug --force --strip-path=1".split(" ")).run())

            r=shelltest("zfs list -H -o name -r -t snapshot test_target1")
            self.assertMultiLineEqual(r,"""
test_target1@test-20101111000000
test_target1/fs1@test-20101111000000
test_target1/fs1/sub@test-20101111000000
test_target1/fs2/sub@test-20101111000000
""")


    def test_exclude_unchanged(self):

        shelltest("zfs snapshot -r test_source1@somesnapshot")

        with mocktime("20101111000000"):
            self.assertFalse(
                ZfsAutobackup(
                    "test test_target1 --verbose --allow-empty --exclude-unchanged=1".split(" ")).run())

        #everything should be excluded, but should not return an error (see #190)
        with mocktime("20101111000001"):
            self.assertFalse(
                ZfsAutobackup(
                    "test test_target1 --verbose --allow-empty --exclude-unchanged=1".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t snapshot test_target1")
        self.assertMultiLineEqual(r, """
test_target1/test_source2/fs2/sub@test-20101111000000
""")

