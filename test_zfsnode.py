from basetest import *


class TestZfsNode(unittest.TestCase):

    def setUp(self):
        prepare_zpools()
        return super().setUp()


    def test_consistent_snapshot(self):
        logger=Logger()
        description="[Source]"
        node=ZfsNode("test", logger, description=description)

        with self.subTest("first snapshot"):
            node.consistent_snapshot(node.selected_datasets, "test-1",100000)
            r=shelltest("zfs list -H -o name -r -t all")
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
            node.consistent_snapshot(node.selected_datasets, "test-2",100000)
            r=shelltest("zfs list -H -o name -r -t all")
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
            node.consistent_snapshot(node.selected_datasets, "test-2",0)
            r=shelltest("zfs list -H -o name -r -t all")
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
        logger=Logger()
        description="[Source]"
        node=ZfsNode("test", logger, description=description)
        s=pformat(node.selected_datasets)
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





if __name__ == '__main__':
    unittest.main()