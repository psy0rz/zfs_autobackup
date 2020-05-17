from basetest import *


class TestZfsAutobackup(unittest.TestCase):

    def setUp(self):
        prepare_zpools()
        return super().setUp()

    def  test_defaults(self):
        self.assertFalse(ZfsAutobackup("test test_target1".split(" ")).run())
