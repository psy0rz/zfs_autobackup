from basetest import *


class TestZfsAutobackup40(unittest2.TestCase):
    """various new 4.0 features"""

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

        target_guid = shelltest("zfs get -H -o value guid test_target1").strip()

        # destroy stuff and see if it still selects the correct ones
        shelltest("zfs destroy test_source2/fs2/sub@test-20101111000001")
        shelltest("zfs destroy test_source1/fs1/sub#test-20101111000001_" + target_guid)

        # replace bookmark with incorrect GUID, should fallback to snapshot
        # NOTE: the incorrect bookmark should be ignored and not destroyed.
        shelltest("zfs destroy test_source1/fs1#test-20101111000001_" + target_guid)
        shelltest("zfs snapshot test_source1/fs1@wrong")
        shelltest("zfs bookmark test_source1/fs1@wrong \#test-20101111000001_" + target_guid)
        shelltest("zfs destroy test_source1/fs1@wrong")

        with mocktime("20101111000002"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t all " + TEST_POOLS)
        self.assertRegexpMatches(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000001
test_source1/fs1@test-20101111000002
test_source1/fs1#test-20101111000001_[0-9]*
test_source1/fs1#test-20101111000002_[0-9]*
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000001
test_source1/fs1/sub@test-20101111000002
test_source1/fs1/sub#test-20101111000002_[0-9]*
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000002
test_source2/fs2/sub#test-20101111000002_[0-9]*
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

        r = shelltest("zfs list -H -o name -r -t all test_source1")
        self.assertRegexpMatches(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000001
test_source1/fs1#test-20101111000001_[0-9]*
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000001
test_source1/fs1/sub#test-20101111000001_[0-9]*
""")

        # disable it (this should not touch the old bookmark)
        with mocktime("20101111000002"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose --allow-empty --no-bookmarks".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t all test_source1")
        self.assertRegexpMatches(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000001
test_source1/fs1@test-20101111000002
test_source1/fs1#test-20101111000001_[0-9]*
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000001
test_source1/fs1/sub@test-20101111000002
test_source1/fs1/sub#test-20101111000001_[0-9]*
""")

        # re-enable (now the old bookmark should be still left alone)
        with mocktime("20101111000003"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t all test_source1")
        self.assertRegexpMatches(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000001
test_source1/fs1@test-20101111000002
test_source1/fs1@test-20101111000003
test_source1/fs1#test-20101111000001_[0-9]*
test_source1/fs1#test-20101111000003_[0-9]*
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000001
test_source1/fs1/sub@test-20101111000002
test_source1/fs1/sub@test-20101111000003
test_source1/fs1/sub#test-20101111000001_[0-9]*
test_source1/fs1/sub#test-20101111000003_[0-9]*
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
        r = shelltest("zfs list -H -r -t all -o name " + TEST_POOLS)

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

    def test_keepsource0target10queuedsend_bookmarks(self):
        """Test if thinner doesnt destroy too much early on if there are no common snapshots YET. Issue #84"""
        # new behavior, with bookmarks. (will delete common snapshot, since there is a bookmark)

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

        r = shelltest("zfs list -H -o name -r -t snapshot,filesystem " + TEST_POOLS)
        self.assertMultiLineEqual(r, """
test_source1
test_source1/fs1
test_source1/fs1/sub
test_source2
test_source2/fs2
test_source2/fs2/sub
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

    def test_migrate_from_mismatching_bookmarkname(self):
        """test migration from a bookmark with a mismatching name."""

        shelltest("zfs snapshot test_source1/fs1@migrate1")
        shelltest("zfs create test_target1/test_source1")
        shelltest("zfs send  test_source1/fs1@migrate1| zfs recv test_target1/test_source1/fs1")

        # make it so there is only the bookmark to migrate from, which a random non-matching name
        shelltest("zfs bookmark test_source1/fs1@migrate1 \#randombookmarkname")
        shelltest("zfs destroy test_source1/fs1@migrate1")

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --test".split(" ")).run())

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t snapshot,filesystem " + TEST_POOLS)
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
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@migrate1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
""")

    def test_migrate_from_mismatching_snapshotname(self):
        """test migration from a snapshot with a mismatching name."""

        shelltest("zfs snapshot test_source1/fs1@migrate1")
        shelltest("zfs create test_target1/test_source1")
        shelltest("zfs send test_source1/fs1@migrate1| zfs recv test_target1/test_source1/fs1")

        # rename it so the names mismatch and guid matching is needed to resolve it
        shelltest("zfs rename test_source1/fs1@migrate1  test_source1/fs1@randomsnapshotname")

        # testmode
        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --test".split(" ")).run())

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t snapshot,filesystem " + TEST_POOLS)
        self.assertMultiLineEqual(r, """
test_source1
test_source1/fs1
test_source1/fs1@randomsnapshotname
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
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@migrate1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
""")

    def test_keep0_bookmarks(self):
        """test if keep-source=0 and keep-target=0 only keeps a bookmark at the source. (contrary to legacy holds-mode)"""

        # backup mode
        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose --keep-source=0 --keep-target=0".split(
                    " ")).run())

        # snapshot mode, will delete 0
        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup(
                "test --no-progress --verbose --keep-source=0 --keep-target=0 --allow-empty".split(" ")).run())

        # snapshot mode, will detete 1
        with mocktime("20101111000002"):
            self.assertFalse(ZfsAutobackup(
                "test --no-progress --verbose --keep-source=0 --keep-target=0 --allow-empty".split(" ")).run())

        # now we have only bookmark 0 and snapshot 2 at source
        r = shelltest("zfs list -H -o name -r -t snapshot,filesystem,bookmark " + TEST_POOLS)
        self.assertRegexpMatches(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000002
test_source1/fs1#test-20101111000000_[0-9]*
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000002
test_source1/fs1/sub#test-20101111000000_[0-9]*
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000002
test_source2/fs2/sub#test-20101111000000_[0-9]*
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
""")

        # make another backup now the source only has bookmarks of 3 left
        with mocktime("20101111000003"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose --keep-source=0 --keep-target=0 --allow-empty --debug".split(
                    " ")).run())

        r = shelltest("zfs list -H -o name -r -t snapshot,filesystem,bookmark " + TEST_POOLS)
        self.assertRegexpMatches(r, """
test_source1
test_source1/fs1
test_source1/fs1#test-20101111000003_[0-9]*
test_source1/fs1/sub
test_source1/fs1/sub#test-20101111000003_[0-9]*
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub#test-20101111000003_[0-9]*
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000003
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000003
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000003
""")

    def test_transfer_thinning_bookmarks(self):
        # test pre/post/during transfer thinning and efficient transfer (no transerring of stuff that gets deleted on target) WITH BOOKMARK SUPPORT

        # less output
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

        with OutputIO() as buf:
            with redirect_stdout(buf):
                # now do thinning and transfer all at once
                with mocktime("20010203000000"):
                    self.assertFalse(ZfsAutobackup(
                        "--keep-source=1d10d --keep-target=1m10m --allow-empty --verbose --clear-mountpoint --other-snapshots test2 test_target1".split(
                            " ")).run())

            print(buf.getvalue())
            self.assertIn(
                """
  [Source] test_source1/fs1/sub@test2-20000101000000: Destroying
  [Source] test_source1/fs1/sub@test2-20010101000000: -> test_target1/test_source1/fs1/sub (new)
  [Source] test_source1/fs1/sub@test2-20010101000000: Destroying
  [Source] test_source1/fs1/sub@other1: -> test_target1/test_source1/fs1/sub
  [Source] test_source1/fs1/sub@test2-20010201000000: -> test_target1/test_source1/fs1/sub
  [Source] test_source1/fs1/sub@other2: -> test_target1/test_source1/fs1/sub
  [Source] test_source1/fs1/sub@test2-20010203000000: -> test_target1/test_source1/fs1/sub
""", buf.getvalue())

        # note: also important that it leaves only the last bookmark on the source.
        r = shelltest("zfs list -H -o name -r -t snapshot,bookmark test_source1 test_target1")
        self.assertRegexpMatches(r, """
test_source1/fs1/sub@other1
test_source1/fs1/sub@test2-20010201000000
test_source1/fs1/sub@other2
test_source1/fs1/sub@test2-20010202000000
test_source1/fs1/sub@test2-20010203000000
test_source1/fs1/sub#test2-20010203000000_[0-9]*
test_target1/test_source1/fs1/sub@test2-20010101000000
test_target1/test_source1/fs1/sub@other1
test_target1/test_source1/fs1/sub@test2-20010201000000
test_target1/test_source1/fs1/sub@other2
test_target1/test_source1/fs1/sub@test2-20010203000000
""")

# TODO
# - check if holds and no-holds function ok when bookmarks are enabled
# -delete bookmarks we have on other snapshots
