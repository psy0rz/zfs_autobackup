from zfs_autobackup.CmdPipe import CmdPipe
from basetest import *
import time


class TestZfsEncryption(unittest2.TestCase):

    def setUp(self):
        prepare_zpools()

    def prepare_encrypted_dataset(self, key, path, unload_key=False):

        # create encrypted source dataset
        shelltest("echo {} > /tmp/zfstest.key".format(key))
        shelltest("zfs create -o keylocation=file:///tmp/zfstest.key -o keyformat=passphrase -o encryption=on {}".format(path))

        if unload_key:
            shelltest("zfs unmount {}".format(path))
            shelltest("zfs unload-key {}".format(path))

        # r=shelltest("dd if=/dev/zero of=/test_source1/fs1/enc1/data.txt bs=200000 count=1")

    def  test_raw(self):
        """send encrypted data unaltered (standard operation)"""

        self.prepare_encrypted_dataset("11111111", "test_source1/fs1/encryptedsource")
        self.prepare_encrypted_dataset("11111111", "test_source1/fs1/encryptedsourcekeyless", unload_key=True) # raw mode shouldn't need a key
        self.prepare_encrypted_dataset("22222222", "test_target1/encryptedtarget")

        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-progress --allow-empty".split(" ")).run())
            self.assertFalse(ZfsAutobackup("test test_target1/encryptedtarget --verbose --no-progress --no-snapshot".split(" ")).run())

        with patch('time.strftime', return_value="20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-progress --allow-empty".split(" ")).run())
            self.assertFalse(ZfsAutobackup("test test_target1/encryptedtarget --verbose --no-progress --no-snapshot".split(" ")).run())

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

    def  test_decrypt(self):
        """decrypt data and store unencrypted (--decrypt)"""

        self.prepare_encrypted_dataset("11111111", "test_source1/fs1/encryptedsource")
        self.prepare_encrypted_dataset("22222222", "test_target1/encryptedtarget")

        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-progress --decrypt".split(" ")).run())
            self.assertFalse(ZfsAutobackup("test test_target1/encryptedtarget --verbose --no-progress --decrypt".split(" ")).run())

        r = shelltest("zfs get -r -t filesystem encryptionroot test_target1")
        self.assertEqual(r, """
NAME                                                           PROPERTY        VALUE                         SOURCE
test_target1                                                   encryptionroot  -                             -
test_target1/encryptedtarget                                   encryptionroot  test_target1/encryptedtarget  -
test_target1/encryptedtarget/test_source1                      encryptionroot  test_target1/encryptedtarget  -
test_target1/encryptedtarget/test_source1/fs1                  encryptionroot  -                             -
test_target1/encryptedtarget/test_source1/fs1/encryptedsource  encryptionroot  -                             -
test_target1/encryptedtarget/test_source1/fs1/sub              encryptionroot  -                             -
test_target1/encryptedtarget/test_source2                      encryptionroot  test_target1/encryptedtarget  -
test_target1/encryptedtarget/test_source2/fs2                  encryptionroot  test_target1/encryptedtarget  -
test_target1/encryptedtarget/test_source2/fs2/sub              encryptionroot  -                             -
test_target1/test_source1                                      encryptionroot  -                             -
test_target1/test_source1/fs1                                  encryptionroot  -                             -
test_target1/test_source1/fs1/encryptedsource                  encryptionroot  -                             -
test_target1/test_source1/fs1/sub                              encryptionroot  -                             -
test_target1/test_source2                                      encryptionroot  -                             -
test_target1/test_source2/fs2                                  encryptionroot  -                             -
test_target1/test_source2/fs2/sub                              encryptionroot  -                             -
""")

    def  test_encrypt(self):
        """send normal data set and store encrypted on the other side (--encrypt) issue #60 """

        self.prepare_encrypted_dataset("11111111", "test_source1/fs1/encryptedsource")
        self.prepare_encrypted_dataset("22222222", "test_target1/encryptedtarget")

        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-progress --encrypt".split(" ")).run())
            self.assertFalse(ZfsAutobackup("test test_target1/encryptedtarget --verbose --no-progress --encrypt".split(" ")).run())

        r = shelltest("zfs get encryption -H -o value test_target1/test_source1/fs1/encryptedsource test_target1/encryptedtarget/test_source1/fs1/encryptedsource")
        self.assertNotIn("off",r)


    def  test_reencrypt(self):
        """decrypt data and reencrypt on the otherside (--decrypt --encrypt) """

        # create encrypted target dataset
        shelltest("echo 12345678 > /tmp/zfstest.key")
        shelltest("zfs create -o keylocation=file:///tmp/zfstest.key -o keyformat=passphrase -o encryption=on test_target1/enc1")

        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1/enc1 --allow-empty --verbose --no-progress".split(" ")).run())
        r = shelltest("zfs get encryption -H -o value test_target1/enc1/test_source1/fs1")
        self.assertNotIn("off",r)
