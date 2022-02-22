from basetest import *
from zfs_autobackup.BlockHasher import BlockHasher


class TestZfsCheck(unittest2.TestCase):

    def setUp(self):
        pass


    def test_volume(self):
        prepare_zpools()

        shelltest("zfs create -V200M test_source1/vol")
        shelltest("zfs snapshot test_source1/vol@test")

        with OutputIO() as buf:
            with redirect_stdout(buf):
                self.assertFalse(ZfsCheck("test_source1/vol@test".split(" "),print_arguments=False).run())

            print(buf.getvalue())
            self.assertEqual("""0	2c2ceccb5ec5574f791d45b63c940cff20550f9a
1	2c2ceccb5ec5574f791d45b63c940cff20550f9a
""", buf.getvalue())


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

        with OutputIO() as buf:
            with redirect_stdout(buf):
                self.assertFalse(ZfsCheck("test_source1@test".split(" "), print_arguments=False).run())

            print(buf.getvalue())
            self.assertEqual("""testfile	0	3c0bf91170d873b8e327d3bafb6bc074580d11b7
dir/testfile	0	2e863f1fcccd6642e4e28453eba10d2d3f74d798
""", buf.getvalue())


    def test_brokenpipe_cleanup_filesystem(self):
        """test if stuff is cleaned up correctly, in debugging mode , when a pipe breaks. """

        prepare_zpools()
        shelltest("cp tests/data/whole /test_source1/testfile")
        shelltest("zfs snapshot test_source1@test")

        #breaks pipe when grep exists:
        #important to use --debug, since that generates extra output which would be problematic if we didnt do correct SIGPIPE handling
        shelltest("python -m zfs_autobackup.ZfsCheck test_source1@test --debug | grep -m1 'Hashing tree'")
        time.sleep(1)

        #should NOT be mounted anymore if cleanup went ok:
        self.assertNotRegex(shelltest("mount"), "test_source1@test")

    def test_brokenpipe_cleanup_volume(self):

        prepare_zpools()
        shelltest("zfs create -V200M test_source1/vol")
        shelltest("zfs snapshot test_source1/vol@test")

        #breaks pipe when grep exists:
        #important to use --debug, since that generates extra output which would be problematic if we didnt do correct SIGPIPE handling
        shelltest("python -m zfs_autobackup.ZfsCheck test_source1/vol@test --debug | grep -m1 'Hashing dev'")
        time.sleep(1)

        r = shelltest("zfs list -H -o name -r -t all " + TEST_POOLS)
        self.assertMultiLineEqual(r, """
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
""")



