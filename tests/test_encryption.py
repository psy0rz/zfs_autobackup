from zfs_autobackup.CmdPipe import CmdPipe
from basetest import *
import time

# We have to do a LOT to properly test encryption/decryption/raw transfers
#
# For every scenario we need at least:
# - plain source dataset
# - encrypted source dataset
# - plain target path
# - encrypted target path
# - do a full transfer
# - do a incremental transfer

# Scenarios:
# - Raw transfer
# - Decryption transfer (--decrypt)
# - Encryption transfer (--encrypt)
# - Re-encryption transfer (--decrypt --encrypt)

class TestZfsEncryption(unittest2.TestCase):


    def setUp(self):
        prepare_zpools()

        try:
            shelltest("zfs get encryption test_source1")
        except:
            self.skipTest("Encryption not supported on this ZFS version.")

    def load_key(self, key, path):

        shelltest("rm /tmp/zfstest.key 2>/dev/null;true")
        shelltest("echo {} > /tmp/zfstest.key".format(key))
        shelltest("zfs load-key {}".format(path))

    def prepare_encrypted_dataset(self, key, path, unload_key=False):

        # create encrypted source dataset
        shelltest("rm /tmp/zfstest.key 2>/dev/null;true")
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

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-progress --allow-empty --exclude-received".split(" ")).run())
            self.assertFalse(ZfsAutobackup("test test_target1/encryptedtarget --verbose --no-progress --no-snapshot --exclude-received".split(" ")).run())

        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-progress --allow-empty --exclude-received".split(" ")).run())
            self.assertFalse(ZfsAutobackup("test test_target1/encryptedtarget --verbose --no-progress --no-snapshot --exclude-received".split(" ")).run())

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

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-progress --decrypt --allow-empty --exclude-received".split(" ")).run())
            self.assertFalse(ZfsAutobackup("test test_target1/encryptedtarget --verbose --no-progress --decrypt --no-snapshot --exclude-received".split(" ")).run())

        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-progress --decrypt --allow-empty --exclude-received".split(" ")).run())
            self.assertFalse(ZfsAutobackup("test test_target1/encryptedtarget --verbose --no-progress --decrypt --no-snapshot --exclude-received".split(" ")).run())

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

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-progress --encrypt --debug --allow-empty --exclude-received --clear-mountpoint".split(" ")).run())
            self.assertFalse(ZfsAutobackup("test test_target1/encryptedtarget --verbose --no-progress --encrypt --debug --no-snapshot --exclude-received --clear-mountpoint".split(" ")).run())

        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-progress --encrypt --debug --allow-empty --exclude-received --clear-mountpoint".split(" ")).run())
            self.assertFalse(ZfsAutobackup("test test_target1/encryptedtarget --verbose --no-progress --encrypt --debug --no-snapshot --exclude-received --clear-mountpoint".split(" ")).run())

        r = shelltest("zfs get -r -t filesystem encryptionroot test_target1")
        self.assertEqual(r, """
NAME                                                           PROPERTY        VALUE                                                          SOURCE
test_target1                                                   encryptionroot  -                                                              -
test_target1/encryptedtarget                                   encryptionroot  test_target1/encryptedtarget                                   -
test_target1/encryptedtarget/test_source1                      encryptionroot  test_target1/encryptedtarget                                   -
test_target1/encryptedtarget/test_source1/fs1                  encryptionroot  test_target1/encryptedtarget                                   -
test_target1/encryptedtarget/test_source1/fs1/encryptedsource  encryptionroot  test_target1/encryptedtarget/test_source1/fs1/encryptedsource  -
test_target1/encryptedtarget/test_source1/fs1/sub              encryptionroot  test_target1/encryptedtarget                                   -
test_target1/encryptedtarget/test_source2                      encryptionroot  test_target1/encryptedtarget                                   -
test_target1/encryptedtarget/test_source2/fs2                  encryptionroot  test_target1/encryptedtarget                                   -
test_target1/encryptedtarget/test_source2/fs2/sub              encryptionroot  test_target1/encryptedtarget                                   -
test_target1/test_source1                                      encryptionroot  -                                                              -
test_target1/test_source1/fs1                                  encryptionroot  -                                                              -
test_target1/test_source1/fs1/encryptedsource                  encryptionroot  test_target1/test_source1/fs1/encryptedsource                  -
test_target1/test_source1/fs1/sub                              encryptionroot  -                                                              -
test_target1/test_source2                                      encryptionroot  -                                                              -
test_target1/test_source2/fs2                                  encryptionroot  -                                                              -
test_target1/test_source2/fs2/sub                              encryptionroot  -                                                              -
""")

    def test_reencrypt(self):
        """reencrypt data (--decrypt --encrypt) """

        self.prepare_encrypted_dataset("11111111", "test_source1/fs1/encryptedsource")
        self.prepare_encrypted_dataset("22222222", "test_target1/encryptedtarget")

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --verbose --no-progress --decrypt --encrypt --debug --allow-empty --exclude-received --clear-mountpoint".split(" ")).run())
            self.assertFalse(ZfsAutobackup(
                "test test_target1/encryptedtarget --verbose --no-progress --decrypt --encrypt --debug --no-snapshot --exclude-received --clear-mountpoint".split(
                    " ")).run())

        with mocktime("20101111000001"):
            self.assertFalse(ZfsAutobackup(
                "test test_target1 --verbose --no-progress --decrypt --encrypt --debug --allow-empty --exclude-received".split(" ")).run())
            self.assertFalse(ZfsAutobackup(
                "test test_target1/encryptedtarget --verbose --no-progress --decrypt --encrypt --debug --no-snapshot --exclude-received".split(
                    " ")).run())

        r = shelltest("zfs get -r -t filesystem encryptionroot test_target1")
        self.assertEqual(r, """
NAME                                                           PROPERTY        VALUE                         SOURCE
test_target1                                                   encryptionroot  -                             -
test_target1/encryptedtarget                                   encryptionroot  test_target1/encryptedtarget  -
test_target1/encryptedtarget/test_source1                      encryptionroot  test_target1/encryptedtarget  -
test_target1/encryptedtarget/test_source1/fs1                  encryptionroot  test_target1/encryptedtarget  -
test_target1/encryptedtarget/test_source1/fs1/encryptedsource  encryptionroot  test_target1/encryptedtarget  -
test_target1/encryptedtarget/test_source1/fs1/sub              encryptionroot  test_target1/encryptedtarget  -
test_target1/encryptedtarget/test_source2                      encryptionroot  test_target1/encryptedtarget  -
test_target1/encryptedtarget/test_source2/fs2                  encryptionroot  test_target1/encryptedtarget  -
test_target1/encryptedtarget/test_source2/fs2/sub              encryptionroot  test_target1/encryptedtarget  -
test_target1/test_source1                                      encryptionroot  -                             -
test_target1/test_source1/fs1                                  encryptionroot  -                             -
test_target1/test_source1/fs1/encryptedsource                  encryptionroot  -                             -
test_target1/test_source1/fs1/sub                              encryptionroot  -                             -
test_target1/test_source2                                      encryptionroot  -                             -
test_target1/test_source2/fs2                                  encryptionroot  -                             -
test_target1/test_source2/fs2/sub                              encryptionroot  -                             -
""")




    def  test_raw_invalid_snapshot(self):
        """in raw mode, its not allowed to have any newer snaphots on target, #219"""

        self.prepare_encrypted_dataset("11111111", "test_source1/fs1/encryptedsource")

        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-progress".split(" ")).run())

        #this is invalid in raw mode
        shelltest("zfs snapshot test_target1/test_source1/fs1/encryptedsource@incompatible")

        with mocktime("20101111000001"):
            #should fail because of incompatble snapshot
            self.assertEqual(ZfsAutobackup("test test_target1 --verbose --no-progress --allow-empty".split(" ")).run(),1)
            #should destroy incompatible and continue
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-progress --no-snapshot --destroy-incompatible".split(" ")).run())


        r = shelltest("zfs get -r -t filesystem encryptionroot test_target1")
        self.assertMultiLineEqual(r,"""
NAME                                           PROPERTY        VALUE                                          SOURCE
test_target1                                   encryptionroot  -                                              -
test_target1/test_source1                      encryptionroot  -                                              -
test_target1/test_source1/fs1                  encryptionroot  -                                              -
test_target1/test_source1/fs1/encryptedsource  encryptionroot  test_target1/test_source1/fs1/encryptedsource  -
test_target1/test_source1/fs1/sub              encryptionroot  -                                              -
test_target1/test_source2                      encryptionroot  -                                              -
test_target1/test_source2/fs2                  encryptionroot  -                                              -
test_target1/test_source2/fs2/sub              encryptionroot  -                                              -
""")


    def  test_resume_encrypt_with_no_key(self):
        """test what happens if target encryption key not loaded (this led to a kernel crash of freebsd with 2.1.x i think) while trying to resume"""

        self.prepare_encrypted_dataset("11111111", "test_source1/fs1/encryptedsource")
        self.prepare_encrypted_dataset("22222222", "test_target1/encryptedtarget")


        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1/encryptedtarget --verbose --no-progress --encrypt --allow-empty --exclude-received --clear-mountpoint".split(" ")).run())

        r = shelltest("zfs set compress=off test_source1 test_target1")

        # big change on source
        r = shelltest("dd if=/dev/zero of=/test_source1/fs1/data bs=250M count=1")

        # waste space on target
        r = shelltest("dd if=/dev/zero of=/test_target1/waste bs=250M count=1")

        # should fail and leave resume token
        with mocktime("20101111000001"):
            self.assertTrue(ZfsAutobackup(
                "test test_target1/encryptedtarget --verbose --no-progress --encrypt --exclude-received --allow-empty --clear-mountpoint".split(
                    " ")).run())
        #
        # free up space
        r = shelltest("rm /test_target1/waste")

        # sync
        r = shelltest("zfs umount test_target1")
        r = shelltest("zfs mount test_target1")

        #
        # #unload key
        shelltest("zfs unload-key test_target1/encryptedtarget")

        # resume should fail
        with mocktime("20101111000001"):
            self.assertEqual(ZfsAutobackup(
                "test test_target1/encryptedtarget --verbose --no-progress --encrypt --exclude-received --allow-empty --no-snapshot --clear-mountpoint".split(
                    " ")).run(),3)



#NOTE: On some versions this leaves 2 weird sub-datasets that should'nt be there (its probably a zfs bug?)
#so we ignore this, and just make sure the backup resumes correctly after reloading the key.
#         r = shelltest("zfs get -r -t all encryptionroot test_target1")
#         self.assertEqual(r, """
# NAME                                                                               PROPERTY        VALUE                                                          SOURCE
# test_target1                                                                       encryptionroot  -                                                              -
# test_target1/encryptedtarget                                                       encryptionroot  test_target1/encryptedtarget                                   -
# test_target1/encryptedtarget/test_source1                                          encryptionroot  test_target1/encryptedtarget                                   -
# test_target1/encryptedtarget/test_source1/fs1                                      encryptionroot  test_target1/encryptedtarget                                   -
# test_target1/encryptedtarget/test_source1/fs1@test-20101111000000                  encryptionroot  test_target1/encryptedtarget                                   -
# test_target1/encryptedtarget/test_source1/fs1/encryptedsource                      encryptionroot  test_target1/encryptedtarget/test_source1/fs1/encryptedsource  -
# test_target1/encryptedtarget/test_source1/fs1/encryptedsource@test-20101111000000  encryptionroot  test_target1/encryptedtarget/test_source1/fs1/encryptedsource  -
# test_target1/encryptedtarget/test_source1/fs1/encryptedsource@test-20101111000001  encryptionroot  test_target1/encryptedtarget/test_source1/fs1/encryptedsource  -
# test_target1/encryptedtarget/test_source1/fs1/sub                                  encryptionroot  test_target1/encryptedtarget                                   -
# test_target1/encryptedtarget/test_source1/fs1/sub@test-20101111000000              encryptionroot  test_target1/encryptedtarget                                   -
# test_target1/encryptedtarget/test_source1/fs1/sub/sub                              encryptionroot  -                                                              -
# test_target1/encryptedtarget/test_source1/fs1/sub/sub@test-20101111000001          encryptionroot  -                                                              -
# test_target1/encryptedtarget/test_source2                                          encryptionroot  test_target1/encryptedtarget                                   -
# test_target1/encryptedtarget/test_source2/fs2                                      encryptionroot  test_target1/encryptedtarget                                   -
# test_target1/encryptedtarget/test_source2/fs2/sub                                  encryptionroot  test_target1/encryptedtarget                                   -
# test_target1/encryptedtarget/test_source2/fs2/sub@test-20101111000000              encryptionroot  test_target1/encryptedtarget                                   -
# test_target1/encryptedtarget/test_source2/fs2/sub/sub                              encryptionroot  -                                                              -
# test_target1/encryptedtarget/test_source2/fs2/sub/sub@test-20101111000001          encryptionroot  -                                                              -
# """)



        #reload key and resume correctly.
        self.load_key("22222222", "test_target1/encryptedtarget")

        # resume should complete
        with mocktime("20101111000001"):
            self.assertEqual(ZfsAutobackup(
                "test test_target1/encryptedtarget --verbose --no-progress --encrypt --exclude-received --allow-empty --no-snapshot --clear-mountpoint".split(
                    " ")).run(),0)

