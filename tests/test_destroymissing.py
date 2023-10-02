
from basetest import *


class TestZfsNode(unittest2.TestCase):

    def setUp(self):
        prepare_zpools()
        self.longMessage=True



    def  test_destroymissing(self):

        #initial backup
        with mocktime("19101111000000"): #1000 years in past
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --no-holds".split(" ")).run())

        with mocktime("20101111000000"): #far in past
            self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --no-holds --allow-empty".split(" ")).run())


        with self.subTest("Should do nothing yet"):
            with OutputIO() as buf:
                with redirect_stdout(buf):
                    self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --no-snapshot --destroy-missing 0s".split(" ")).run())

                print(buf.getvalue())
                self.assertNotIn(": Destroy missing", buf.getvalue())


        with self.subTest("missing dataset of us that still has children"):

            #just deselect it so it counts as 'missing'
            shelltest("zfs set autobackup:test=child test_source1/fs1")

            with OutputIO() as buf:
                with redirect_stdout(buf), redirect_stderr(buf):
                        self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --no-snapshot --destroy-missing 0s".split(" ")).run())

                print(buf.getvalue())
                #should have done the snapshot cleanup for destoy missing:
                self.assertIn("fs1@test-19101111000000: Destroying", buf.getvalue())

                self.assertIn("fs1: Destroy missing: Still has children here.", buf.getvalue())

            shelltest("zfs inherit autobackup:test test_source1/fs1")


        with self.subTest("Normal destroyed leaf"):
            shelltest("zfs destroy -r test_source1/fs1/sub")

            #wait for deadline of last snapshot
            with OutputIO() as buf:
                with redirect_stdout(buf):
                    #100y: lastest should not be old enough, while second to latest snapshot IS old enough:
                    self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --no-snapshot --destroy-missing 100y".split(" ")).run())

                print(buf.getvalue())
                self.assertIn(": Waiting for deadline", buf.getvalue())

            #past deadline, destroy
            with OutputIO() as buf:
                with redirect_stdout(buf):
                    self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --no-snapshot --destroy-missing 1y".split(" ")).run())

                print(buf.getvalue())
                self.assertIn("sub: Destroying", buf.getvalue())


        with self.subTest("Leaf with other snapshot still using it"):
            shelltest("zfs destroy -r test_source1/fs1")
            shelltest("zfs snapshot -r test_target1/test_source1/fs1@other1")


            with OutputIO() as buf:
                with redirect_stdout(buf):
                    self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --no-snapshot --destroy-missing 0s".split(" ")).run())

                print(buf.getvalue())

                #cant finish because still in use:
                self.assertIn("fs1: Destroy missing: Still in use", buf.getvalue())

            shelltest("zfs destroy test_target1/test_source1/fs1@other1")


        with self.subTest("In use by clone"):
            shelltest("zfs clone test_target1/test_source1/fs1@test-20101111000000 test_target1/clone1")

            with OutputIO() as buf:
                with redirect_stdout(buf), redirect_stderr(buf):
                        self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --no-snapshot --destroy-missing 0s".split(" ")).run())

                print(buf.getvalue())
                #now tries to destroy our own last snapshot (before the final destroy of the dataset)
                self.assertIn("fs1@test-20101111000000: Destroying", buf.getvalue())
                #but cant finish because still in use:
                self.assertIn("fs1: Error during --destroy-missing", buf.getvalue())

            shelltest("zfs destroy test_target1/clone1")


        with self.subTest("Should leave test_source1 parent"):

            with OutputIO() as buf:
                with redirect_stdout(buf), redirect_stderr(buf):
                        self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --no-snapshot --destroy-missing 0s".split(" ")).run())

                print(buf.getvalue())
                #should have done the snapshot cleanup for destoy missing:
                self.assertIn("fs1: Destroying", buf.getvalue())

            with OutputIO() as buf:
                with redirect_stdout(buf), redirect_stderr(buf):
                        self.assertFalse(ZfsAutobackup("test test_target1 --no-progress --verbose --no-snapshot --destroy-missing 0s".split(" ")).run())

                print(buf.getvalue())
                #on second run it sees the dangling ex-parent but doesnt know what to do with it (since it has no own snapshot)
                self.assertIn("test_source1: Destroy missing: has no snapshots made by us", buf.getvalue())




        #end result
        r=shelltest("zfs list -H -o name -r -t all test_target1")
        self.assertMultiLineEqual(r,"""
test_target1
test_target1/test_source1
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-19101111000000
test_target1/test_source2/fs2/sub@test-20101111000000
""")
