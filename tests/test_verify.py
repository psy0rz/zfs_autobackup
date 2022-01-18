from zfs_autobackup.CmdPipe import CmdPipe
from basetest import *
import time

# test zfs-verify:
# - when there is no common snapshot at all
# - when encryption key not loaded
# - on datasets:
#   - that are correct
#   - that are different
#   - that are not mounted
#   - that are mounted
#   - that are mounted on the "wrong" place
# - on zvols
#  - that are correct
#  - that are different
#

class TestZfsEncryption(unittest2.TestCase):


    def setUp(self):
        prepare_zpools()

        shelltest("zfs create test_source1/fs1/ok_filesystem")
        shelltest("cp *.py /test_source1/fs1/ok_filesystem")
        shelltest("zfs create -V 1M test_source1/fs1/ok_zvol")
        shelltest("dd if=/dev/urandom of=/dev/zvol/test_source1/fs1/ok_zvol count=1 bs=512k")

        with patch('time.strftime', return_value="test-20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-progress".split(" ")).run())

        # make sure we cant accidently compare current data
        shelltest("rm /test_source1/fs1/ok_filesystem/*")
        shelltest("dd if=/dev/zero of=/dev/zvol/test_source1/fs1/ok_zvol count=1 bs=512k")

    def test_verify(self):

        return

        r = shelltest("zfs get -r -t filesystem encryptionroot test_target1")
        self.assertMultiLineEqual(r,"""
NAME                                                                  PROPERTY        VALUE                                                                 SOURCE
test_target1                                                          encryptionroot  -                                                                     -
test_target1/encryptedtarget                                          encryptionroot  test_target1/encryptedtarget                                          -
test_target1/encryptedtarget/test_source1                             encryptionroot  test_target1/encryptedtarget                                          -
test_target1/encryptedtarget/test_source1/fs1                         encryptionroot  -                                                                     -
test_target1/encryptedtarget/test_source1/fs1/encryptedsource         encryptionroot  test_target1/encryptedtarget/test_source1/fs1/encryptedsource         -
test_target1/encryptedtarget/test_source1/fs1/encryptedsourcekeyless  encryptionroot  test_target1/encryptedtarget/test_source1/fs1/encryptedsourcekeyless  -
test_target1/encryptedtarget/test_source1/fs1/sub                     encryptionroot  -                                                                     -
test_target1/encryptedtarget/test_source2                             encryptionroot  test_target1/encryptedtarget                                          -
test_target1/encryptedtarget/test_source2/fs2                         encryptionroot  test_target1/encryptedtarget                                          -
test_target1/encryptedtarget/test_source2/fs2/sub                     encryptionroot  -                                                                     -
test_target1/test_source1                                             encryptionroot  -                                                                     -
test_target1/test_source1/fs1                                         encryptionroot  -                                                                     -
test_target1/test_source1/fs1/encryptedsource                         encryptionroot  test_target1/test_source1/fs1/encryptedsource                         -
test_target1/test_source1/fs1/encryptedsourcekeyless                  encryptionroot  test_target1/test_source1/fs1/encryptedsourcekeyless                  -
test_target1/test_source1/fs1/sub                                     encryptionroot  -                                                                     -
test_target1/test_source2                                             encryptionroot  -                                                                     -
test_target1/test_source2/fs2                                         encryptionroot  -                                                                     -
test_target1/test_source2/fs2/sub                                     encryptionroot  -                                                                     -
""")

