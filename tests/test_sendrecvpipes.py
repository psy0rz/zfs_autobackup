import zfs_autobackup.compressors
from basetest import *
import time


class TestSendRecvPipes(unittest2.TestCase):
    """test input/output pipes for zfs send and recv"""

    def setUp(self):
        prepare_zpools()
        self.longMessage = True

    def test_send_basics(self):
        """send basics (remote/local send pipe)"""

        with self.subTest("local local pipe"):
            with mocktime("20101111000000"):
                self.assertFalse(ZfsAutobackup(
                    ["test", "test_target1", "--allow-empty", "--exclude-received", "--no-holds", "--no-progress", "--clear-mountpoint",
                     "--send-pipe=dd bs=1M", "--recv-pipe=dd bs=2M"]).run())

            shelltest("zfs destroy -r test_target1/test_source1/fs1/sub")

        with self.subTest("remote local pipe"):
            with mocktime("20101111000001"):
                self.assertFalse(ZfsAutobackup(
                    ["test", "test_target1", "--allow-empty", "--exclude-received", "--no-holds", "--no-progress",
                     "--ssh-source=localhost", "--send-pipe=dd bs=1M", "--recv-pipe=dd bs=2M"]).run())

            shelltest("zfs destroy -r test_target1/test_source1/fs1/sub")

        with self.subTest("local remote pipe"):
            with mocktime("20101111000002"):
                self.assertFalse(ZfsAutobackup(
                    ["test", "test_target1", "--allow-empty", "--exclude-received", "--no-holds", "--no-progress",
                     "--ssh-target=localhost", "--send-pipe=dd bs=1M", "--recv-pipe=dd bs=2M"]).run())

            shelltest("zfs destroy -r test_target1/test_source1/fs1/sub")

        with self.subTest("remote remote pipe"):
            with mocktime("20101111000003"):
                self.assertFalse(ZfsAutobackup(
                    ["test", "test_target1", "--allow-empty", "--exclude-received", "--no-holds", "--no-progress",
                     "--ssh-source=localhost", "--ssh-target=localhost", "--send-pipe=dd bs=1M",
                     "--recv-pipe=dd bs=2M"]).run())

        r = shelltest("zfs list -H -o name -r -t all test_target1")
        self.assertMultiLineEqual(r, """
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1@test-20101111000001
test_target1/test_source1/fs1@test-20101111000002
test_target1/test_source1/fs1@test-20101111000003
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source1/fs1/sub@test-20101111000001
test_target1/test_source1/fs1/sub@test-20101111000002
test_target1/test_source1/fs1/sub@test-20101111000003
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
test_target1/test_source2/fs2/sub@test-20101111000001
test_target1/test_source2/fs2/sub@test-20101111000002
test_target1/test_source2/fs2/sub@test-20101111000003
""")

    def test_compress(self):
        """send basics (remote/local send pipe)"""

        for compress in zfs_autobackup.compressors.COMPRESS_CMDS.keys():
            with self.subTest("compress " + compress):
                with mocktime("20101111000000"):
                    self.assertFalse(ZfsAutobackup(
                        ["test", "test_target1", "--exclude-received", "--no-holds", "--no-progress", "--verbose",
                         "--compress=" + compress]).run())

                shelltest("zfs destroy -r test_target1/test_source1/fs1/sub")

    def test_buffer(self):
        """test different buffer configurations"""

        with self.subTest("local local pipe"):
            with mocktime("20101111000000"):
                self.assertFalse(ZfsAutobackup(
                    ["test", "test_target1", "--allow-empty", "--exclude-received", "--no-holds", "--no-progress", "--clear-mountpoint", "--buffer=1M"]).run())

            shelltest("zfs destroy -r test_target1/test_source1/fs1/sub")

        with self.subTest("remote local pipe"):
            with mocktime("20101111000001"):
                self.assertFalse(ZfsAutobackup(
                    ["test", "test_target1", "--allow-empty", "--verbose", "--exclude-received", "--no-holds",
                     "--no-progress", "--ssh-source=localhost", "--buffer=1M"]).run())

            shelltest("zfs destroy -r test_target1/test_source1/fs1/sub")

        with self.subTest("local remote pipe"):
            with mocktime("20101111000002"):
                self.assertFalse(ZfsAutobackup(
                    ["test", "test_target1", "--allow-empty", "--exclude-received", "--no-holds", "--no-progress",
                     "--ssh-target=localhost", "--buffer=1M"]).run())

            shelltest("zfs destroy -r test_target1/test_source1/fs1/sub")

        with self.subTest("remote remote pipe"):
            with mocktime("20101111000003"):
                self.assertFalse(ZfsAutobackup(
                    ["test", "test_target1", "--allow-empty", "--exclude-received", "--no-holds", "--no-progress",
                     "--ssh-source=localhost", "--ssh-target=localhost", "--buffer=1M"]).run())

        r = shelltest("zfs list -H -o name -r -t all test_target1")
        self.assertMultiLineEqual(r, """
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1@test-20101111000001
test_target1/test_source1/fs1@test-20101111000002
test_target1/test_source1/fs1@test-20101111000003
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source1/fs1/sub@test-20101111000001
test_target1/test_source1/fs1/sub@test-20101111000002
test_target1/test_source1/fs1/sub@test-20101111000003
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
test_target1/test_source2/fs2/sub@test-20101111000001
test_target1/test_source2/fs2/sub@test-20101111000002
test_target1/test_source2/fs2/sub@test-20101111000003
""")

    def test_rate(self):
        """test rate limit"""

        start = time.time()
        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup(
                ["test", "test_target1", "--exclude-received", "--no-holds", "--no-progress", "--rate=50k"]).run())

        # not a great way of verifying but it works.
        self.assertGreater(time.time() - start, 5)
