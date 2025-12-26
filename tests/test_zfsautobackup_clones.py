from basetest import *

class TestZfsAutobackupClones(unittest2.TestCase):
    """clones support"""

    def setUp(self):
        prepare_zpools()
        subprocess.check_call("zfs snapshot test_source1/fs1/sub@snap1", shell=True)
        subprocess.check_call("zfs clone test_source1/fs1/sub@snap1 test_source1/fs1/subclone", shell=True)
        self.longMessage=True

    def test_clones_never(self):
        self.assertFalse(ZfsAutobackup(("--no-progress", "--verbose", "--allow-empty", "test", "test_target1")).run())
        self.assertMultiLineEqual(shelltest("zfs list -H -o name,origin -r " + TEST_POOLS), """
test_source1\t-
test_source1/fs1\t-
test_source1/fs1/sub\t-
test_source1/fs1/subclone\ttest_source1/fs1/sub@snap1
test_source2\t-
test_source2/fs2\t-
test_source2/fs2/sub\t-
test_source2/fs3\t-
test_source2/fs3/sub\t-
test_target1\t-
test_target1/test_source1\t-
test_target1/test_source1/fs1\t-
test_target1/test_source1/fs1/sub\t-
test_target1/test_source1/fs1/subclone\t-
test_target1/test_source2\t-
test_target1/test_source2/fs2\t-
test_target1/test_source2/fs2/sub\t-
""")

    def test_clones_simple(self):
        # --clones=simple is not enough, you need to transfer the origin snapshot as well!
        self.assertEqual(1, ZfsAutobackup(("--no-progress", "--verbose", "--allow-empty", "--clones", "simple",
                                           "test", "test_target1")).run())

    def test_clones_simple_with_other_snapshots(self):
        # --clones=simple is not enough, you need to transfer the origin snapshot as well!
        # For example, using --other-snapshots
        self.assertFalse(ZfsAutobackup(("--no-progress", "--verbose", "--allow-empty", "--clones", "simple", "--other-snapshots",
                                        "test", "test_target1")).run())
        self.assertMultiLineEqual(shelltest("zfs list -H -o name,origin -r " + TEST_POOLS), """
test_source1\t-
test_source1/fs1\t-
test_source1/fs1/sub\t-
test_source1/fs1/subclone\ttest_source1/fs1/sub@snap1
test_source2\t-
test_source2/fs2\t-
test_source2/fs2/sub\t-
test_source2/fs3\t-
test_source2/fs3/sub\t-
test_target1\t-
test_target1/test_source1\t-
test_target1/test_source1/fs1\t-
test_target1/test_source1/fs1/sub\t-
test_target1/test_source1/fs1/subclone\ttest_target1/test_source1/fs1/sub@snap1
test_target1/test_source2\t-
test_target1/test_source2/fs2\t-
test_target1/test_source2/fs2/sub\t-
""")

