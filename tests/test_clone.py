from basetest import *


class TestZfsAutobackupClone(unittest2.TestCase):
    """ZFS clone replication: source datasets that are themselves clones get
    replicated to the target as clones of the corresponding target-side origin
    snapshot, via 'zfs send -i <origin_snap> <clone>@<snap>'."""

    def setUp(self):
        prepare_zpools()
        self.longMessage = True

    def test_clone_happy_path(self):
        """Single run, both origin and clone selected. The origin snapshot is
        auto-included because a clone in the selection depends on it, so the user
        doesn't need --other-snapshots."""

        shelltest("zfs snapshot test_source1/fs1@base")
        shelltest("zfs clone test_source1/fs1@base test_source1/fs1_clone")
        shelltest("zfs set autobackup:test=true test_source1/fs1_clone")

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t snapshot,filesystem " + TEST_POOLS)
        self.assertMultiLineEqual(r, """
test_source1
test_source1/fs1
test_source1/fs1@base
test_source1/fs1@test-20101111000000
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000000
test_source1/fs1_clone
test_source1/fs1_clone@test-20101111000000
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000000
test_source2/fs3
test_source2/fs3/sub
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@base
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source1/fs1_clone
test_target1/test_source1/fs1_clone@test-20101111000000
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
""")

        origin = shelltest("zfs get -H -o value origin test_target1/test_source1/fs1_clone").strip()
        self.assertEqual(origin, "test_target1/test_source1/fs1@base")

    def test_clone_origin_not_on_target_falls_back_to_full_send(self):
        """If the clone's origin isn't replicated, warn and fall back to full send."""

        shelltest("zfs inherit autobackup:test test_source1/fs1")
        shelltest("zfs snapshot test_source1/fs1@base")
        shelltest("zfs clone test_source1/fs1@base test_source1/fs1_clone")
        shelltest("zfs set autobackup:test=true test_source1/fs1_clone")

        with OutputIO() as buf:
            with redirect_stdout(buf), redirect_stderr(buf):
                with mocktime("20101111000000"):
                    self.assertFalse(ZfsAutobackup(
                        "test test_target1 --no-progress --verbose".split(" ")).run())

            output = buf.getvalue()
            print(output)
            self.assertIn("Cannot replicate as clone", output)

        origin = shelltest("zfs get -H -o value origin test_target1/test_source1/fs1_clone").strip()
        self.assertEqual(origin, "-")

    def test_clone_after_initial_origin_backup(self):
        """First run backs up the origin; second run replicates a clone of it."""

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose".split(" ")).run())

        shelltest("zfs clone test_source1/fs1@test-20101111000000 test_source1/fs1_clone")
        shelltest("zfs set autobackup:test=true test_source1/fs1_clone")

        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        origin = shelltest("zfs get -H -o value origin test_target1/test_source1/fs1_clone").strip()
        self.assertEqual(origin, "test_target1/test_source1/fs1@test-20101111000000")

    def test_no_clone_flag_disables_clone_replication(self):
        """--no-clone reverts to the previous full-send behaviour for clones."""

        shelltest("zfs snapshot test_source1/fs1@base")
        shelltest("zfs clone test_source1/fs1@base test_source1/fs1_clone")
        shelltest("zfs set autobackup:test=true test_source1/fs1_clone")

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose --no-clone".split(" ")).run())

        origin = shelltest("zfs get -H -o value origin test_target1/test_source1/fs1_clone").strip()
        self.assertEqual(origin, "-")

    def test_subsequent_incremental_after_clone_send(self):
        """A second run after a clone has been sent is a plain incremental."""

        shelltest("zfs snapshot test_source1/fs1@base")
        shelltest("zfs clone test_source1/fs1@base test_source1/fs1_clone")
        shelltest("zfs set autobackup:test=true test_source1/fs1_clone")

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose".split(" ")).run())

        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose --allow-empty".split(" ")).run())

        r = shelltest("zfs list -H -o name -t snapshot test_target1/test_source1/fs1_clone")
        self.assertMultiLineEqual(r, """
test_target1/test_source1/fs1_clone@test-20101111000000
test_target1/test_source1/fs1_clone@test-20101111000001
""")

        origin = shelltest("zfs get -H -o value origin test_target1/test_source1/fs1_clone").strip()
        self.assertEqual(origin, "test_target1/test_source1/fs1@base")

    def test_topological_sort_processes_origin_before_clone(self):
        """When the clone alphabetically precedes its origin, the topo sort still
        processes the origin first so the clone can pin to its target origin."""

        shelltest("zfs snapshot test_source1/fs1@base")
        shelltest("zfs clone test_source1/fs1@base test_source1/aaa_clone")
        shelltest("zfs set autobackup:test=true test_source1/aaa_clone")

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose".split(" ")).run())

        origin = shelltest("zfs get -H -o value origin test_target1/test_source1/aaa_clone").strip()
        self.assertEqual(origin, "test_target1/test_source1/fs1@base")

    def test_test_mode_makes_no_changes_for_clones(self):
        """--test must not modify anything, even when clone handling is exercised."""

        shelltest("zfs snapshot test_source1/fs1@base")
        shelltest("zfs clone test_source1/fs1@base test_source1/fs1_clone")
        shelltest("zfs set autobackup:test=true test_source1/fs1_clone")

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --no-progress --verbose --test".split(" ")).run())

        r = shelltest("zfs list -H -o name -r -t snapshot,filesystem test_target1")
        self.assertMultiLineEqual(r, """
test_target1
""")

    def test_promoted_clone_origin_on_namespace_descendant_warns_and_falls_back(self):
        """After 'zfs promote', the original parent becomes a clone whose origin lives
        on one of its own namespace descendants. zfs recv can't land a clone-creating
        stream on top of the placeholder we have to create for that descendant, so we
        detect this upfront, warn, and fall back to a full send (which will itself
        fail at the placeholder — user can destroy target and rerun, or promote the
        source dataset to flip the lineage back)."""

        shelltest("zfs snapshot test_source1/fs1@s1")
        shelltest("zfs clone test_source1/fs1@s1 test_source1/fs1/inner_clone")
        shelltest("zfs promote test_source1/fs1/inner_clone")

        origin = shelltest("zfs get -H -o value origin test_source1/fs1").strip()
        self.assertEqual(origin, "test_source1/fs1/inner_clone@s1")

        with OutputIO() as buf:
            with redirect_stdout(buf), redirect_stderr(buf):
                with mocktime("20101111000000"):
                    result = ZfsAutobackup(
                        "test test_target1 --no-progress --verbose".split(" ")).run()

            output = buf.getvalue()
            print(output)

        self.assertTrue(result)
        self.assertIn("lives on a namespace descendant", output)
