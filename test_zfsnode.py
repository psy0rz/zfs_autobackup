from basetest import *


class TestZfsNode(unittest.TestCase):

    def setUp(self):
        prepare_zpools()
        return super().setUp()


    def test_consistent_snapshot(self):
        logger=Logger()
        description="[Source]"
        node=ZfsNode("test", logger, description=description)

        with self.subTest("### no changes, no snapshot"):
            node.consistent_snapshot(node.selected_datasets, "snap1", 100000)
            s=pformat(subprocess.check_call("zfs list -r -t all" , shell=True))
            print(s)





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