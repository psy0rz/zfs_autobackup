from basetest import *
import time

class TestSendRecvPipes(unittest2.TestCase):
    """test input/output pipes for zfs send and recv"""

    def setUp(self):
        prepare_zpools()
        self.longMessage=True



    def test_send_basics(self):
        """send basics (remote/local send pipe)"""

        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup(["test", "test_target1", "--no-progress", "--send-pipe=dd bs=1M",  "--send-pipe=dd bs=2M"]).run())

        with patch('time.strftime', return_value="20101111000001"):
            self.assertFalse(ZfsAutobackup(["test", "test_target1", "--no-progress", "--ssh-source=localhost", "--send-pipe=dd bs=1M",  "--send-pipe=dd bs=2M"]).run())

#             r=shelltest("zfs list -H -o name -r -t snapshot test_target1")
#             #NOTE: it wont backup test_target1/a/test_source2/fs2/sub to test_target1/b since it doesnt have the zfs_autobackup property anymore.
#             self.assertMultiLineEqual(r,"""
# test_target1/a/test_source1/fs1@test-20101111000000
# test_target1/a/test_source1/fs1/sub@test-20101111000000
# test_target1/a/test_source2/fs2/sub@test-20101111000000
# test_target1/b/test_source1/fs1@test-20101111000000
# test_target1/b/test_source1/fs1/sub@test-20101111000000
# test_target1/b/test_source2/fs2/sub@test-20101111000000
# test_target1/b/test_target1/a/test_source1/fs1@test-20101111000000
# test_target1/b/test_target1/a/test_source1/fs1/sub@test-20101111000000
# """)