from basetest import *
from zfs_autobackup.BlockHasher import BlockHasher


# sha1 sums of files, (bs=4096)
# da39a3ee5e6b4b0d3255bfef95601890afd80709  empty
# 642027d63bb0afd7e0ba197f2c66ad03e3d70de1  partial
# 3c0bf91170d873b8e327d3bafb6bc074580d11b7  whole
# 2e863f1fcccd6642e4e28453eba10d2d3f74d798  whole2
# 959e6b58078f0cfd2fb3d37e978fda51820473ff  whole_whole2
# 309ffffba2e1977d12f3b7469971f30d28b94bd8  whole_whole2_partial


class TestTreeHasher(unittest2.TestCase):

    def test_treehasher(self):
        shelltest("rm -rf /tmp/treehashertest; mkdir /tmp/treehashertest")
        shelltest("cp tests/data/whole /tmp/treehashertest")
        shelltest("mkdir /tmp/treehashertest/emptydir")
        shelltest("mkdir /tmp/treehashertest/dir")
        shelltest("cp tests/data/whole_whole2_partial /tmp/treehashertest/dir")

        # it should ignore these:
        shelltest("ln -s / /tmp/treehashertest/symlink")
        shelltest("mknod /tmp/treehashertest/c c 1 1")
        shelltest("mknod /tmp/treehashertest/b b 1 1")
        shelltest("mkfifo /tmp/treehashertest/f")


        block_hasher = BlockHasher(count=1, skip=0)
        tree_hasher = TreeHasher(block_hasher)
        with self.subTest("Test output, count 1, skip 0"):
            self.assertEqual(list(tree_hasher.generate("/tmp/treehashertest")), [
                ('whole', 0, '3c0bf91170d873b8e327d3bafb6bc074580d11b7'),
                ('dir/whole_whole2_partial', 0, '3c0bf91170d873b8e327d3bafb6bc074580d11b7'),
                ('dir/whole_whole2_partial', 1, '2e863f1fcccd6642e4e28453eba10d2d3f74d798'),
                ('dir/whole_whole2_partial', 2, '642027d63bb0afd7e0ba197f2c66ad03e3d70de1')
            ])

        block_hasher = BlockHasher(count=1, skip=1)
        tree_hasher = TreeHasher(block_hasher)
        with self.subTest("Test output, count 1, skip 1"):
            self.assertEqual(list(tree_hasher.generate("/tmp/treehashertest")), [
                ('whole', 0, '3c0bf91170d873b8e327d3bafb6bc074580d11b7'),
                # ('dir/whole_whole2_partial', 0, '3c0bf91170d873b8e327d3bafb6bc074580d11b7'),
                ('dir/whole_whole2_partial', 1, '2e863f1fcccd6642e4e28453eba10d2d3f74d798'),
                # ('dir/whole_whole2_partial', 2, '642027d63bb0afd7e0ba197f2c66ad03e3d70de1')
            ])



        block_hasher = BlockHasher(count=2)
        tree_hasher = TreeHasher(block_hasher)

        with self.subTest("Test output, count 2, skip 0"):
            self.assertEqual(list(tree_hasher.generate("/tmp/treehashertest")), [
                ('whole', 0, '3c0bf91170d873b8e327d3bafb6bc074580d11b7'),
                ('dir/whole_whole2_partial', 0, '959e6b58078f0cfd2fb3d37e978fda51820473ff'),
                ('dir/whole_whole2_partial', 1, '642027d63bb0afd7e0ba197f2c66ad03e3d70de1')
            ])

        with self.subTest("Test compare"):
            generator = tree_hasher.generate("/tmp/treehashertest")
            errors = list(tree_hasher.compare("/tmp/treehashertest", generator))
            self.assertEqual(errors, [])

        with self.subTest("Test mismatch"):
            generator = list(tree_hasher.generate("/tmp/treehashertest"))
            shelltest("cp tests/data/whole2 /tmp/treehashertest/whole")

            self.assertEqual(list(tree_hasher.compare("/tmp/treehashertest", generator)),
                             [('whole',
                               0,
                               '3c0bf91170d873b8e327d3bafb6bc074580d11b7',
                               '2e863f1fcccd6642e4e28453eba10d2d3f74d798')])

        with self.subTest("Test missing file compare"):
            generator = list(tree_hasher.generate("/tmp/treehashertest"))
            shelltest("rm /tmp/treehashertest/whole")

            self.assertEqual(list(tree_hasher.compare("/tmp/treehashertest", generator)),
                             [('whole', '-', '-', "ERROR: [Errno 2] No such file or directory: '/tmp/treehashertest/whole'")])


