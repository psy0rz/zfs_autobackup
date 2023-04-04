from basetest import *
from zfs_autobackup.LogStub import LogStub
from zfs_autobackup.ExecuteNode import ExecuteError


class TestZfsNode(unittest2.TestCase):

    def setUp(self):
        prepare_zpools()
        # return super().setUp()

    def test_consistent_snapshot(self):
        logger = LogStub()
        description = "[Source]"
        node = ZfsNode(utc=False, snapshot_time_format="test-%Y%m%d%H%M%S", hold_name="zfs_autobackup:test", logger=logger, description=description)

        with self.subTest("first snapshot"):
            (selected_datasets, excluded_datasets)=node.selected_datasets(property_name="autobackup:test", exclude_paths=[], exclude_received=False,
                                   exclude_unchanged=0)
            node.consistent_snapshot(selected_datasets, "test-20101111000001", 100000)
            r = shelltest("zfs list -H -o name -r -t all " + TEST_POOLS)
            self.assertEqual(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000001
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000001
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000001
test_source2/fs3
test_source2/fs3/sub
test_target1
""")

        with self.subTest("second snapshot, no changes, no snapshot"):
            (selected_datasets, excluded_datasets)=node.selected_datasets(property_name="autobackup:test", exclude_paths=[], exclude_received=False,
                                   exclude_unchanged=0)
            node.consistent_snapshot(selected_datasets, "test-20101111000002", 1)
            r = shelltest("zfs list -H -o name -r -t all " + TEST_POOLS)
            self.assertEqual(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000001
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000001
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000001
test_source2/fs3
test_source2/fs3/sub
test_target1
""")

        with self.subTest("second snapshot, no changes, empty snapshot"):
            (selected_datasets, excluded_datasets) =node.selected_datasets(property_name="autobackup:test", exclude_paths=[], exclude_received=False, exclude_unchanged=0)
            node.consistent_snapshot(selected_datasets, "test-20101111000002", 0)
            r = shelltest("zfs list -H -o name -r -t all " + TEST_POOLS)
            self.assertEqual(r, """
test_source1
test_source1/fs1
test_source1/fs1@test-20101111000001
test_source1/fs1@test-20101111000002
test_source1/fs1/sub
test_source1/fs1/sub@test-20101111000001
test_source1/fs1/sub@test-20101111000002
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-20101111000001
test_source2/fs2/sub@test-20101111000002
test_source2/fs3
test_source2/fs3/sub
test_target1
""")

    def test_consistent_snapshot_prepostcmds(self):
        logger = LogStub()
        description = "[Source]"
        node = ZfsNode(utc=False, snapshot_time_format="test", hold_name="test", logger=logger, description=description, debug_output=True)

        with self.subTest("Test if all cmds are executed correctly (no failures)"):
            with OutputIO() as buf:
                with redirect_stdout(buf):
                    (selected_datasets, excluded_datasets) =node.selected_datasets(property_name="autobackup:test", exclude_paths=[], exclude_received=False, exclude_unchanged=0)
                    node.consistent_snapshot(selected_datasets, "test-1",
                                             0,
                                             pre_snapshot_cmds=["echo pre1", "echo pre2"],
                                             post_snapshot_cmds=["echo post1 >&2", "echo post2 >&2"]
                                             )

                self.assertIn("STDOUT > pre1", buf.getvalue())
                self.assertIn("STDOUT > pre2", buf.getvalue())
                self.assertIn("STDOUT > post1", buf.getvalue())
                self.assertIn("STDOUT > post2", buf.getvalue())


        with self.subTest("Failure in the middle, only pre1 and both post1 and post2 should be executed, no snapshot should be attempted"):
            with OutputIO() as buf:
                with redirect_stdout(buf):
                    with self.assertRaises(ExecuteError):
                        (selected_datasets, excluded_datasets) =node.selected_datasets(property_name="autobackup:test", exclude_paths=[], exclude_received=False, exclude_unchanged=0)
                        node.consistent_snapshot(selected_datasets, "test-1",
                                                 0,
                                                 pre_snapshot_cmds=["echo pre1", "false", "echo pre2"],
                                                 post_snapshot_cmds=["echo post1", "false", "echo post2"]
                                                 )

                print(buf.getvalue())
                self.assertIn("STDOUT > pre1", buf.getvalue())
                self.assertNotIn("STDOUT > pre2", buf.getvalue())
                self.assertIn("STDOUT > post1", buf.getvalue())
                self.assertIn("STDOUT > post2", buf.getvalue())

        with self.subTest("Snapshot fails"):
            with OutputIO() as buf:
                with redirect_stdout(buf):
                    with self.assertRaises(ExecuteError):
                        #same snapshot name as before so it fails
                        (selected_datasets, excluded_datasets) =node.selected_datasets(property_name="autobackup:test", exclude_paths=[], exclude_received=False, exclude_unchanged=0)
                        node.consistent_snapshot(selected_datasets, "test-1",
                                                 0,
                                                 pre_snapshot_cmds=["echo pre1", "echo pre2"],
                                                 post_snapshot_cmds=["echo post1", "echo post2"]
                                                 )

                print(buf.getvalue())
                self.assertIn("STDOUT > pre1", buf.getvalue())
                self.assertIn("STDOUT > pre2", buf.getvalue())
                self.assertIn("STDOUT > post1", buf.getvalue())
                self.assertIn("STDOUT > post2", buf.getvalue())

    def test_timestamps(self):
        # Assert that timestamps keep relative order both for utc and for localtime
        logger = LogStub()
        description = "[Source]"
        node_local = ZfsNode(utc=False, snapshot_time_format="test-%Y%m%d%H%M%S", hold_name="zfs_autobackup:test", logger=logger, description=description)
        node_utc = ZfsNode(utc=True, snapshot_time_format="test-%Y%m%d%H%M%S", hold_name="zfs_autobackup:test", logger=logger, description=description)

        for node in [node_local, node_utc]:
            with self.subTest("timestamp ordering " + ("utc" if node == node_utc else "localtime")):
                dataset_a = ZfsDataset(node,"test_source1@test-20101111000001")
                dataset_b = ZfsDataset(node,"test_source1@test-20101111000002")
                dataset_c = ZfsDataset(node,"test_source1@test-20240101020202")
                self.assertGreater(dataset_b.timestamp, dataset_a.timestamp)
                self.assertGreater(dataset_c.timestamp, dataset_b.timestamp)


    def test_getselected(self):

        # should be excluded by property
        shelltest("zfs create test_source1/fs1/subexcluded")
        shelltest("zfs set autobackup:test=false test_source1/fs1/subexcluded")

        # only select parent
        shelltest("zfs create test_source1/fs1/onlyparent")
        shelltest("zfs create test_source1/fs1/onlyparent/child")
        shelltest("zfs set autobackup:test=parent test_source1/fs1/onlyparent")

        # should be excluded by being unchanged
        shelltest("zfs create test_source1/fs1/unchanged")
        shelltest("zfs snapshot test_source1/fs1/unchanged@somesnapshot")

        logger = LogStub()
        description = "[Source]"
        node = ZfsNode(utc=False, snapshot_time_format="test-%Y%m%d%H%M%S", hold_name="zfs_autobackup:test", logger=logger, description=description)
        (selected_datasets, excluded_datasets)=node.selected_datasets(property_name="autobackup:test", exclude_paths=[], exclude_received=False,
                               exclude_unchanged=1)
        s = pformat(selected_datasets)
        print(s)

        # basics
        self.assertEqual(s, """[(local): test_source1/fs1,
 (local): test_source1/fs1/onlyparent,
 (local): test_source1/fs1/sub,
 (local): test_source2/fs2/sub]""")


    def test_validcommand(self):
        logger = LogStub()
        description = "[Source]"
        node = ZfsNode(utc=False, snapshot_time_format="test-%Y%m%d%H%M%S", hold_name="zfs_autobackup:test", logger=logger, description=description)

        with self.subTest("test invalid option"):
            self.assertFalse(node.valid_command(["zfs", "send", "--invalid-option", "nonexisting"]))
        with self.subTest("test valid option"):
            self.assertTrue(node.valid_command(["zfs", "send", "-v", "nonexisting"]))

    def test_supportedsendoptions(self):
        logger = LogStub()
        description = "[Source]"
        node = ZfsNode(utc=False, snapshot_time_format="test-%Y%m%d%H%M%S", hold_name="zfs_autobackup:test", logger=logger, description=description)
        # -D propably always supported
        self.assertGreater(len(node.supported_send_options), 0)

    def test_supportedrecvoptions(self):
        logger = LogStub()
        description = "[Source]"
        # NOTE: this could hang via ssh if we dont close filehandles properly. (which was a previous bug)
        node = ZfsNode(utc=False, snapshot_time_format="test-%Y%m%d%H%M%S", hold_name="zfs_autobackup:test", logger=logger, description=description, ssh_to='localhost')
        self.assertIsInstance(node.supported_recv_options, list)


if __name__ == '__main__':
    unittest.main()
