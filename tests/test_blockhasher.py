from basetest import *
from zfs_autobackup.BlockHasher import BlockHasher


# make VERY sure this works correctly under all circumstances.

# sha1 sums of files, (bs=4096)
# da39a3ee5e6b4b0d3255bfef95601890afd80709  empty
# 642027d63bb0afd7e0ba197f2c66ad03e3d70de1  partial
# 3c0bf91170d873b8e327d3bafb6bc074580d11b7  whole
# 2e863f1fcccd6642e4e28453eba10d2d3f74d798  whole2
# 959e6b58078f0cfd2fb3d37e978fda51820473ff  whole_whole2
# 309ffffba2e1977d12f3b7469971f30d28b94bd8  whole_whole2_partial

class TestBlockHasher(unittest2.TestCase):

    def setUp(self):
        pass

    def test_empty(self):
        block_hasher = BlockHasher(count=1)
        self.assertEqual(
            list(block_hasher.generate("tests/data/empty")),
            []
        )

    def test_partial(self):
        block_hasher = BlockHasher(count=1)
        self.assertEqual(
            list(block_hasher.generate("tests/data/partial")),
            [(0, "642027d63bb0afd7e0ba197f2c66ad03e3d70de1")]
        )

    def test_whole(self):
        block_hasher = BlockHasher(count=1)
        self.assertEqual(
            list(block_hasher.generate("tests/data/whole")),
            [(0, "3c0bf91170d873b8e327d3bafb6bc074580d11b7")]
        )

    def test_whole2(self):
        block_hasher = BlockHasher(count=1)
        self.assertEqual(
            list(block_hasher.generate("tests/data/whole_whole2")),
            [
                (0, "3c0bf91170d873b8e327d3bafb6bc074580d11b7"),
                (1, "2e863f1fcccd6642e4e28453eba10d2d3f74d798")
            ]
        )

    def test_wwp(self):
        block_hasher = BlockHasher(count=1)
        self.assertEqual(
            list(block_hasher.generate("tests/data/whole_whole2_partial")),
            [
                (0, "3c0bf91170d873b8e327d3bafb6bc074580d11b7"),  # whole
                (1, "2e863f1fcccd6642e4e28453eba10d2d3f74d798"),  # whole2
                (2, "642027d63bb0afd7e0ba197f2c66ad03e3d70de1")  # partial
            ]
        )

    def test_wwp_count2(self):
        block_hasher = BlockHasher(count=2)
        self.assertEqual(
            list(block_hasher.generate("tests/data/whole_whole2_partial")),
            [
                (0, "959e6b58078f0cfd2fb3d37e978fda51820473ff"),  # whole_whole2
                (1, "642027d63bb0afd7e0ba197f2c66ad03e3d70de1")  # partial
            ]
        )

    def test_big(self):
        block_hasher = BlockHasher(count=10)
        self.assertEqual(
            list(block_hasher.generate("tests/data/whole_whole2_partial")),
            [
                (0, "309ffffba2e1977d12f3b7469971f30d28b94bd8"),  # whole_whole2_partial
            ])

    def test_blockhash_compare(self):
        #no errors
        block_hasher = BlockHasher(count=1)
        generator = block_hasher.generate("tests/data/whole_whole2_partial")
        self.assertEqual([], list(block_hasher.compare("tests/data/whole_whole2_partial", generator)))

        #compare file is smaller (EOF errors)
        block_hasher = BlockHasher(count=1)
        generator = block_hasher.generate("tests/data/whole_whole2_partial")
        self.assertEqual(
            [(1, '2e863f1fcccd6642e4e28453eba10d2d3f74d798', 'EOF'),
             (2, '642027d63bb0afd7e0ba197f2c66ad03e3d70de1', 'EOF')],
            list(block_hasher.compare("tests/data/whole", generator)))

        #no errors, huge chunks
        block_hasher = BlockHasher(count=10)
        generator = block_hasher.generate("tests/data/whole_whole2_partial")
        self.assertEqual([], list(block_hasher.compare("tests/data/whole_whole2_partial", generator)))

        # different order to make sure seek functions are ok
        block_hasher = BlockHasher(count=1)
        checksums = list(block_hasher.generate("tests/data/whole_whole2_partial"))
        checksums.reverse()
        self.assertEqual([], list(block_hasher.compare("tests/data/whole_whole2_partial", checksums)))

    def test_skip1(self):
        block_hasher = BlockHasher(count=1, skip=1)
        self.assertEqual(
            list(block_hasher.generate("tests/data/whole_whole2_partial")),
            [
                (0, "3c0bf91170d873b8e327d3bafb6bc074580d11b7"),  # whole
                # (1, "2e863f1fcccd6642e4e28453eba10d2d3f74d798"),  # whole2
                (2, "642027d63bb0afd7e0ba197f2c66ad03e3d70de1")  # partial
            ]
        )

        #should continue the pattern on the next file:
        self.assertEqual(
            list(block_hasher.generate("tests/data/whole_whole2_partial")),
            [
                # (0, "3c0bf91170d873b8e327d3bafb6bc074580d11b7"),  # whole
                (1, "2e863f1fcccd6642e4e28453eba10d2d3f74d798"),  # whole2
                # (2, "642027d63bb0afd7e0ba197f2c66ad03e3d70de1")  # partial
            ]
        )

    def test_skip6(self):
        block_hasher = BlockHasher(count=1, skip=6)
        self.assertEqual(
            list(block_hasher.generate("tests/data/whole_whole2_partial")),
            [
                (0, "3c0bf91170d873b8e327d3bafb6bc074580d11b7"),  # whole
                # (1, "2e863f1fcccd6642e4e28453eba10d2d3f74d798"),  # whole2
                # (2, "642027d63bb0afd7e0ba197f2c66ad03e3d70de1")  # partial
            ]
        )

        #all blocks of next file are skipped
        self.assertEqual(
            list(block_hasher.generate("tests/data/whole_whole2_partial")),
            [
                # (0, "3c0bf91170d873b8e327d3bafb6bc074580d11b7"),  # whole
                # (1, "2e863f1fcccd6642e4e28453eba10d2d3f74d798"),  # whole2
                # (2, "642027d63bb0afd7e0ba197f2c66ad03e3d70de1")  # partial
            ]
        )

        #first block of this one is the 6th to be skipped:
        self.assertEqual(
            list(block_hasher.generate("tests/data/whole_whole2_partial")),
            [
                # (0, "3c0bf91170d873b8e327d3bafb6bc074580d11b7"),  # whole
                (1, "2e863f1fcccd6642e4e28453eba10d2d3f74d798"),  # whole2
                # (2, "642027d63bb0afd7e0ba197f2c66ad03e3d70de1")  # partial
            ]
        )

    #NOTE: compare doesnt use skip. thats the job of its input generator