from basetest import *


class TestZfsAutobackup(unittest.TestCase):

    def setUp(self):
        prepare_zpools()
        return super().setUp()

    def  test_defaults(self):
        with self.subTest("defaults with full verbose and debug"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --debug".split(" ")).run())

        with self.subTest("bare defaults"):
            self.assertFalse(ZfsAutobackup("test test_target1".split(" ")).run())
