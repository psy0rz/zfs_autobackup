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
            self.assertFalse(ZfsAutobackup(["test", "test_target1", "--no-progress", "--send-pipe=dd bs=1M",  "--recv-pipe=dd bs=2M"]).run())

        with patch('time.strftime', return_value="20101111000001"):
            self.assertFalse(ZfsAutobackup(["test", "test_target1", "--no-progress", "--ssh-source=localhost", "--send-pipe=dd bs=1M",  "--recv-pipe=dd bs=2M"]).run())

        with patch('time.strftime', return_value="20101111000001"):
            self.assertFalse(ZfsAutobackup(["test", "test_target1", "--no-progress", "--ssh-target=localhost", "--send-pipe=dd bs=1M",  "--recv-pipe=dd bs=2M"]).run())

        with patch('time.strftime', return_value="20101111000001"):
            self.assertFalse(ZfsAutobackup(["test", "test_target1", "--no-progress", "--ssh-source=localhost", "--ssh-target=localhost", "--send-pipe=dd bs=1M",  "--recv-pipe=dd bs=2M"]).run())
