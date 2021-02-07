
from basetest import *


class TestZfsNode(unittest2.TestCase):

    def setUp(self):
        prepare_zpools()
        self.longMessage=True

    # #resume initial backup
    # def test_keepsource0(self):

    #     #somehow only specifying --allow-empty --keep-source 0 failed:
    #     with patch('time.strftime', return_value="20101111000000"):
    #         self.assertFalse(ZfsAutobackup("test test_target1 --verbose --allow-empty --keep-source 0".split(" ")).run())

    #     with patch('time.strftime', return_value="20101111000001"):
    #         self.assertFalse(ZfsAutobackup("test test_target1 --verbose --allow-empty --keep-source 0".split(" ")).run())
