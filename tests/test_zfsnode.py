from basetest import *
from zfs_autobackup.LogStub import LogStub



class TestZfsNode(unittest2.TestCase):

    def setUp(self):
        prepare_zpools()
        # return super().setUp()


    def test_consistent_snapshot(self):
        logger=LogStub()
        description="[Source]"
        node=ZfsNode("test", logger, description=description)

        with self.subTest("first snapshot"):
            node.consistent_snapshot(node.selected_datasets(exclude_paths=[], exclude_received=False), "test-1",100000)
            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            self.assertEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-1
test_source1/fs1/sub
test_source1/fs1/sub@test-1
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-1
test_source2/fs3
test_source2/fs3/sub
test_target1
""")


        with self.subTest("second snapshot, no changes, no snapshot"):
            node.consistent_snapshot(node.selected_datasets(exclude_paths=[], exclude_received=False), "test-2",1)
            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            self.assertEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-1
test_source1/fs1/sub
test_source1/fs1/sub@test-1
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-1
test_source2/fs3
test_source2/fs3/sub
test_target1
""")

        with self.subTest("second snapshot, no changes, empty snapshot"):
            node.consistent_snapshot(node.selected_datasets(exclude_paths=[], exclude_received=False), "test-2",0)
            r=shelltest("zfs list -H -o name -r -t all "+TEST_POOLS)
            self.assertEqual(r,"""
test_source1
test_source1/fs1
test_source1/fs1@test-1
test_source1/fs1@test-2
test_source1/fs1/sub
test_source1/fs1/sub@test-1
test_source1/fs1/sub@test-2
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs2/sub@test-1
test_source2/fs2/sub@test-2
test_source2/fs3
test_source2/fs3/sub
test_target1
""")


    def test_getselected(self):
        logger=LogStub()
        description="[Source]"
        node=ZfsNode("test", logger, description=description)
        s=pformat(node.selected_datasets(exclude_paths=[], exclude_received=False))
        print(s)

        #basics
        self.assertEqual (s, """[(local): test_source1/fs1,
 (local): test_source1/fs1/sub,
 (local): test_source2/fs2/sub]""")

        #caching, so expect same result after changing it
        subprocess.check_call("zfs set autobackup:test=true test_source2/fs3", shell=True)
        self.assertEqual (s, """[(local): test_source1/fs1,
 (local): test_source1/fs1/sub,
 (local): test_source2/fs2/sub]""")


    def test_validcommand(self):
        logger=LogStub()
        description="[Source]"
        node=ZfsNode("test", logger, description=description)


        with self.subTest("test invalid option"):
            self.assertFalse(node.valid_command(["zfs", "send", "--invalid-option", "nonexisting"]))
        with self.subTest("test valid option"):
            self.assertTrue(node.valid_command(["zfs", "send", "-v", "nonexisting"]))

    def test_supportedsendoptions(self):
        logger=LogStub()
        description="[Source]"
        node=ZfsNode("test", logger, description=description)
        # -D propably always supported
        self.assertGreater(len(node.supported_send_options),0)


    def test_supportedrecvoptions(self):
        logger=LogStub()
        description="[Source]"
        #NOTE: this could hang via ssh if we dont close filehandles properly. (which was a previous bug)
        node=ZfsNode("test", logger, description=description, ssh_to='localhost')
        self.assertIsInstance(node.supported_recv_options, list)



if __name__ == '__main__':
    unittest.main()
