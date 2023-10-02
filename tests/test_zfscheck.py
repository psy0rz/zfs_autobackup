from os.path import exists

from basetest import *
from zfs_autobackup.BlockHasher import BlockHasher


class TestZfsCheck(unittest2.TestCase):

    def setUp(self):
        pass


    def test_volume(self):

        if exists("/.dockerenv"):
            self.skipTest("FIXME: zfscheck volumes not supported in docker yet")

        prepare_zpools()

        shelltest("zfs create -V200M test_source1/vol")
        shelltest("zfs snapshot test_source1/vol@test")

        with self.subTest("Generate"):
            with OutputIO() as buf:
                with redirect_stdout(buf):
                    self.assertFalse(ZfsCheck("test_source1/vol@test".split(" "),print_arguments=False).run())

                print(buf.getvalue())
                self.assertEqual("""0	2c2ceccb5ec5574f791d45b63c940cff20550f9a
1	2c2ceccb5ec5574f791d45b63c940cff20550f9a
""", buf.getvalue())

                #store on disk for next step, add one error.
                with open("/tmp/testhashes", "w") as fh:
                    fh.write(buf.getvalue()+"1\t2c2ceccb5ec5574f791d45b63c940cff20550f9X")

        with self.subTest("Compare"):
            with OutputIO() as buf:
                with redirect_stdout(buf):
                    self.assertEqual(1, ZfsCheck("test_source1/vol@test --check=/tmp/testhashes".split(" "),print_arguments=False).run())
                print(buf.getvalue())
                self.assertEqual("Chunk 1 failed: 2c2ceccb5ec5574f791d45b63c940cff20550f9X 2c2ceccb5ec5574f791d45b63c940cff20550f9a\n", buf.getvalue())

    def test_filesystem(self):
        prepare_zpools()

        shelltest("cp tests/data/whole /test_source1/testfile")
        shelltest("mkdir /test_source1/emptydir")
        shelltest("mkdir /test_source1/dir")
        shelltest("cp tests/data/whole2 /test_source1/dir/testfile")

        #it should ignore these:
        shelltest("ln -s / /test_source1/symlink")
        shelltest("mknod /test_source1/c c 1 1")
        shelltest("mknod /test_source1/b b 1 1")
        shelltest("mkfifo /test_source1/f")

        shelltest("zfs snapshot test_source1@test")
        ZfsCheck("test_source1@test --debug".split(" "), print_arguments=False).run()
        with self.subTest("Generate"):
            with OutputIO() as buf:
                with redirect_stdout(buf):
                    self.assertFalse(ZfsCheck("test_source1@test".split(" "), print_arguments=False).run())

                print(buf.getvalue())
                self.assertEqual("""testfile	0	3c0bf91170d873b8e327d3bafb6bc074580d11b7
dir/testfile	0	2e863f1fcccd6642e4e28453eba10d2d3f74d798
""", buf.getvalue())

                #store on disk for next step, add error
                with open("/tmp/testhashes", "w") as fh:
                    fh.write(buf.getvalue()+"dir/testfile	0	2e863f1fcccd6642e4e28453eba10d2d3f74d79X")

        with self.subTest("Compare"):
            with OutputIO() as buf:
                with redirect_stdout(buf):
                    self.assertEqual(1, ZfsCheck("test_source1@test --check=/tmp/testhashes".split(" "),print_arguments=False).run())

                print(buf.getvalue())
                self.assertEqual("dir/testfile: Chunk 0 failed: 2e863f1fcccd6642e4e28453eba10d2d3f74d79X 2e863f1fcccd6642e4e28453eba10d2d3f74d798\n", buf.getvalue())

    def test_file(self):

        with self.subTest("Generate"):
            with OutputIO() as buf:
                with redirect_stdout(buf):
                    self.assertFalse(ZfsCheck("tests/data/whole".split(" "), print_arguments=False).run())

                print(buf.getvalue())
                self.assertEqual("""0	3c0bf91170d873b8e327d3bafb6bc074580d11b7
""", buf.getvalue())

                # store on disk for next step, add error
                with open("/tmp/testhashes", "w") as fh:
                    fh.write(buf.getvalue()+"0	3c0bf91170d873b8e327d3bafb6bc074580d11bX")

        with self.subTest("Compare"):
            with OutputIO() as buf:
                with redirect_stdout(buf):
                    self.assertEqual(1,ZfsCheck("tests/data/whole --check=/tmp/testhashes".split(" "), print_arguments=False).run())
                print(buf.getvalue())
                self.assertEqual("Chunk 0 failed: 3c0bf91170d873b8e327d3bafb6bc074580d11bX 3c0bf91170d873b8e327d3bafb6bc074580d11b7\n", buf.getvalue())

    def test_tree(self):
        shelltest("rm -rf /tmp/testtree; mkdir /tmp/testtree")
        shelltest("cp tests/data/whole /tmp/testtree")
        shelltest("cp tests/data/whole_whole2 /tmp/testtree")
        shelltest("cp tests/data/whole2 /tmp/testtree")
        shelltest("cp tests/data/partial /tmp/testtree")
        shelltest("cp tests/data/whole_whole2_partial /tmp/testtree")

        ####################################
        with self.subTest("Generate, skip 1"):
            with OutputIO() as buf:
                with redirect_stdout(buf):
                    self.assertFalse(ZfsCheck("/tmp/testtree --skip=1".split(" "), print_arguments=False).run())

                #since order varies, just check count (there is one empty line for some reason, only when testing like this)
                print(buf.getvalue().split("\n"))
                self.assertEqual(len(buf.getvalue().split("\n")),4)

        ######################################
        with self.subTest("Compare, all incorrect, skip 1"):

            # store on disk for next step, add error
            with open("/tmp/testhashes", "w") as fh:
                fh.write("""
partial	0	642027d63bb0afd7e0ba197f2c66ad03e3d70deX
whole	0	3c0bf91170d873b8e327d3bafb6bc074580d11bX
whole2	0	2e863f1fcccd6642e4e28453eba10d2d3f74d79X
whole_whole2	0	959e6b58078f0cfd2fb3d37e978fda51820473fX
whole_whole2_partial	0	309ffffba2e1977d12f3b7469971f30d28b94bdX
""")

            with OutputIO() as buf:
                with redirect_stdout(buf):
                    self.assertEqual(ZfsCheck("/tmp/testtree --check=/tmp/testhashes --skip=1".split(" "), print_arguments=False).run(), 3)

                print(buf.getvalue())
                self.assertMultiLineEqual("""partial: Chunk 0 failed: 642027d63bb0afd7e0ba197f2c66ad03e3d70deX 642027d63bb0afd7e0ba197f2c66ad03e3d70de1
whole2: Chunk 0 failed: 2e863f1fcccd6642e4e28453eba10d2d3f74d79X 2e863f1fcccd6642e4e28453eba10d2d3f74d798
whole_whole2_partial: Chunk 0 failed: 309ffffba2e1977d12f3b7469971f30d28b94bdX 309ffffba2e1977d12f3b7469971f30d28b94bd8
""",buf.getvalue())

        ####################################
        with self.subTest("Generate"):
            with OutputIO() as buf:
                with redirect_stdout(buf):
                    self.assertFalse(ZfsCheck("/tmp/testtree".split(" "), print_arguments=False).run())

                #file order on disk can vary, so sort it..
                sorted=buf.getvalue().split("\n")
                sorted.sort()
                sorted="\n".join(sorted)+"\n"

                print(sorted)
                self.assertEqual("""
partial	0	642027d63bb0afd7e0ba197f2c66ad03e3d70de1
whole	0	3c0bf91170d873b8e327d3bafb6bc074580d11b7
whole2	0	2e863f1fcccd6642e4e28453eba10d2d3f74d798
whole_whole2	0	959e6b58078f0cfd2fb3d37e978fda51820473ff
whole_whole2_partial	0	309ffffba2e1977d12f3b7469971f30d28b94bd8
""", sorted)

                # store on disk for next step, add error
                with open("/tmp/testhashes", "w") as fh:
                    fh.write(buf.getvalue() + "whole_whole2_partial	0	309ffffba2e1977d12f3b7469971f30d28b94bdX")

        ####################################
        with self.subTest("Compare"):
            with OutputIO() as buf:
                with redirect_stdout(buf):
                    self.assertEqual(1, ZfsCheck("/tmp/testtree --check=/tmp/testhashes".split(" "),
                                                 print_arguments=False).run())
                print(buf.getvalue())
                self.assertEqual(
                    "whole_whole2_partial: Chunk 0 failed: 309ffffba2e1977d12f3b7469971f30d28b94bdX 309ffffba2e1977d12f3b7469971f30d28b94bd8\n",
                    buf.getvalue())

    def test_brokenpipe_cleanup_filesystem(self):
        """test if stuff is cleaned up correctly, in debugging mode , when a pipe breaks. """

        prepare_zpools()
        shelltest("cp tests/data/whole /test_source1/testfile")
        shelltest("zfs snapshot test_source1@test")

        #breaks pipe when head exists
        #important to use --debug, since that generates extra output which would be problematic if we didnt do correct SIGPIPE handling
        shelltest("python -m zfs_autobackup.ZfsCheck test_source1@test --debug | head -n1")

        #should NOT be mounted anymore if cleanup went ok:
        self.assertNotRegex(shelltest("mount"), "test_source1@test")

    def test_brokenpipe_cleanup_volume(self):
        if exists("/.dockerenv"):
            self.skipTest("FIXME: zfscheck volumes not supported in docker yet")

        prepare_zpools()
        shelltest("zfs create -V200M test_source1/vol")
        shelltest("zfs snapshot test_source1/vol@test")

        #breaks pipe when grep exists:
        #important to use --debug, since that generates extra output which would be problematic if we didnt do correct SIGPIPE handling
        shelltest("python -m zfs_autobackup.ZfsCheck test_source1/vol@test --debug| grep -m1 'Hashing file'")
        # time.sleep(1)

        r = shelltest("zfs list -H -o name -r -t all " + TEST_POOLS)
        self.assertMultiLineEqual("""
test_source1
test_source1/fs1
test_source1/fs1/sub
test_source1/vol
test_source1/vol@test
test_source2
test_source2/fs2
test_source2/fs2/sub
test_source2/fs3
test_source2/fs3/sub
test_target1
""",r )



