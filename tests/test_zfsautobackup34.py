from basetest import *


class TestZfsAutobackup34(unittest2.TestCase):
    """various new 3.4 features"""

    def setUp(self):
        prepare_zpools()
        self.longMessage = True

    def test_no_bookmark_source_support(self):
        """test if everything is fine when source has no bookmark support (has no features at all even)"""

        subprocess.check_call("zpool destroy test_source1", shell=True)
        subprocess.check_call("zpool create -d test_source1 /dev/ram0", shell=True)
        shelltest("zpool get all test_source1")
        subprocess.check_call("zfs create -p test_source1/fs1/sub", shell=True)  # recreate with no features at all
        subprocess.check_call("zfs set autobackup:test=true test_source1/fs1", shell=True)

        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        # should fallback on holds on the source snapshot
        r = shelltest("zfs holds test_source1/fs1@test-20101111000001")
        self.assertIn("zfs_autobackup:test", r)

    def test_no_bookmark_target_support(self):
        """test if everything is fine when target has no bookmark support (has no features at all even)"""
        # NOTE: not sure if its ok if only the source supports bookmarks, so currently zfs-autobackup requires both sides to support bookmarks to enable it.

        subprocess.check_call("zpool destroy test_target1", shell=True)
        subprocess.check_call("zpool create -d test_target1 /dev/ram2", shell=True)

        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        # should fallback on holds on the source snapshot
        r = shelltest("zfs holds test_source1/fs1@test-20101111000001")
        self.assertIn("zfs_autobackup:test", r)

    def test_select_bookmark_or_snapshot(self):
        """test if zfs autobackup chooses the most recent common matching dataset when there are both bookmarks and snapshots, some with the wrong GUID"""

        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        # destroy stuff and see if it still selects the correct ones
        shelltest("zfs destroy test_source2/fs2/sub@test-20101111000001")
        shelltest("zfs destroy test_source1/fs1/sub#test-20101111000001")

        # bookmark with incorrect GUID, should fallback to snapshot
        shelltest("zfs destroy test_source1/fs1#test-20101111000001")
        shelltest("zfs snapshot test_source1/fs1@wrong")
        shelltest("zfs bookmark test_source1/fs1@wrong \#test-20101111000001")
        shelltest("zfs destroy test_source1/fs1@wrong")

        with mocktime("20101111000002"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t snapshot,filesystem " + TEST_POOLS)
        self.assertMultiLineEqual(r, """
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

        # while we're here, check that there are no holds on source common snapshot (since bookmarks replace holds on source side)
        r = shelltest("zfs holds test_source2/fs2/sub@test-20101111000002")
        self.assertNotIn("zfs_autobackup:test", r)

    def test_disable_bookmarks(self):
        """test if we can disable it on an existing backup with bookmarks, with --no-bookmarks and get the old behaviour (holds on source)"""

        # first with bookmarks enabled
        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t snapshot,filesystem test_source1")
        self.assertMultiLineEqual(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000001
test_source1/fs1#test-20101111000001
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000001
test_source1/fs1/sub#test-20101111000001
""")

        # disable it
        with mocktime("20101111000002"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose --allow-empty --no-bookmarks".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t snapshot,filesystem test_source1")
        self.assertMultiLineEqual(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000001
test_source1/fs1@test-20101111000002
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000001
test_source1/fs1/sub@test-20101111000002
""")

        # re-enable
        with mocktime("20101111000003"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t snapshot,filesystem test_source1")
        self.assertMultiLineEqual(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000001
test_source1/fs1@test-20101111000002
test_source1/fs1@test-20101111000003
test_source1/fs1#test-20101111000003
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000001
test_source1/fs1/sub@test-20101111000002
test_source1/fs1/sub@test-20101111000003
test_source1/fs1/sub#test-20101111000003
""")

    def test_tags(self):
        with mocktime("20101111000001"):
            self.assertFalse(
                ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --tag test1".split(" ")).run())

        with mocktime("20101111000002"):
            self.assertFalse(
                ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty --tag test2".split(" ")).run())

        with mocktime("20101111000003"):
            # make sure the thinner sees and cleans up the old snaphots that have a tag
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose --allow-empty --keep-source=2 --keep-target=2".split(
                    " ")).run())

        r = shelltest("zfs list -H -r -t snapshot -o name " + TEST_POOLS)

        self.assertMultiLineEqual(r, """
test_source1/fs1@test-20101111000002_test2
test_source1/fs1@test-20101111000003
test_source1/fs1/sub@test-20101111000002_test2
test_source1/fs1/sub@test-20101111000003
test_source2/fs2/sub@test-20101111000002_test2
test_source2/fs2/sub@test-20101111000003
test_target1/test_source1/fs1@test-20101111000002_test2
test_target1/test_source1/fs1@test-20101111000003
test_target1/test_source1/fs1/sub@test-20101111000002_test2
test_target1/test_source1/fs1/sub@test-20101111000003
test_target1/test_source2/fs2/sub@test-20101111000002_test2
test_target1/test_source2/fs2/sub@test-20101111000003
""")

    def test_double_send_bookmark(self):
        """test sending the same snaphots to 2 targets, and check if they each use their own bookmark and delete them correctly."""

        shelltest("zfs create test_target1/a")
        shelltest("zfs create test_target1/b")

        # full
        with mocktime("20101111000001"):
            self.assertFalse(
                ZfsAutobackup(
                    "test test_target1/a --no-progress --verbose --allow-empty --debug --tag tagA".split(" ")).run())

        # increment, should be from bookmark
        with mocktime("20101111000002"):
            self.assertFalse(
                ZfsAutobackup(
                    "test test_target1/a --no-progress --verbose --allow-empty --tag tagB".split(" ")).run())

        # to target b, now each has one full + two incrementals, which should be from their own bookmarks.
        with mocktime("20101111000003"):
            self.assertFalse(
                ZfsAutobackup(
                    "test test_target1/b --no-progress --verbose --allow-empty".split(
                        " ")).run())

        # result:
        # for target a the bookmarks should be at 20101111000002, for target b the bookmarks should be at 20101111000003
        r = shelltest("zfs list -H -r -t snapshot,filesystem -o name " + TEST_POOLS)

        self.assertRegexpMatches(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000001_tagA
test_source1/fs1@test-20101111000002_tagB
test_source1/fs1@test-20101111000003
test_source1/fs1#test-20101111000002_[0-9]*
test_source1/fs1#test-20101111000003_[0-9]*
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000001_tagA
test_source1/fs1/sub@test-20101111000002_tagB
test_source1/fs1/sub@test-20101111000003
test_source1/fs1/sub#test-20101111000002_[0-9]*
test_source1/fs1/sub#test-20101111000003_[0-9]*
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000001_tagA
test_source2/fs2/sub@test-20101111000002_tagB
test_source2/fs2/sub@test-20101111000003
test_source2/fs2/sub#test-20101111000002_[0-9]*
test_source2/fs2/sub#test-20101111000003_[0-9]*
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/a
test_target1/a/test_source1
test_target1/a/test_source1/fs1
test_target1/a/test_source1/fs1@test-20101111000001_tagA
test_target1/a/test_source1/fs1@test-20101111000002_tagB
test_target1/a/test_source1/fs1/sub
test_target1/a/test_source1/fs1/sub@test-20101111000001_tagA
test_target1/a/test_source1/fs1/sub@test-20101111000002_tagB
test_target1/a/test_source2
test_target1/a/test_source2/fs2
test_target1/a/test_source2/fs2/sub
test_target1/a/test_source2/fs2/sub@test-20101111000001_tagA
test_target1/a/test_source2/fs2/sub@test-20101111000002_tagB
test_target1/b
test_target1/b/test_source1
test_target1/b/test_source1/fs1
test_target1/b/test_source1/fs1@test-20101111000001_tagA
test_target1/b/test_source1/fs1@test-20101111000002_tagB
test_target1/b/test_source1/fs1@test-20101111000003
test_target1/b/test_source1/fs1/sub
test_target1/b/test_source1/fs1/sub@test-20101111000001_tagA
test_target1/b/test_source1/fs1/sub@test-20101111000002_tagB
test_target1/b/test_source1/fs1/sub@test-20101111000003
test_target1/b/test_source2
test_target1/b/test_source2/fs2
test_target1/b/test_source2/fs2/sub
test_target1/b/test_source2/fs2/sub@test-20101111000001_tagA
test_target1/b/test_source2/fs2/sub@test-20101111000002_tagB
test_target1/b/test_source2/fs2/sub@test-20101111000003
""")

    def test_missing_common_bookmark(self):
        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose --allow-empty --no-holds".split(" ")).run())

        # remove common bookmark
        bookmark = shelltest("zfs list -H -o name -t bookmark test_source1/fs1").strip()
        shelltest("zfs destroy " + bookmark)

        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())
