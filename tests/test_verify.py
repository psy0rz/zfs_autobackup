
from basetest import *


# test zfs-verify:
# - when there is no common snapshot at all
# - when encryption key not loaded
# - --test mode
# - --fs-compare methods
# - on snapshots of datasets:
#   - that are correct
#   - that are different
# - on snapshots of zvols
#  - that are correct
#  - that are different
# - test all directions (local, remote/local, local/remote, remote/remote)
#

class TestZfsVerify(unittest2.TestCase):


    def setUp(self):
        self.skipTest("WIP")

        prepare_zpools()

        #create actual test files and data
        shelltest("zfs create test_source1/fs1/ok_filesystem")
        shelltest("cp tests/*.py /test_source1/fs1/ok_filesystem")

        shelltest("zfs create test_source1/fs1/bad_filesystem")
        shelltest("cp tests/*.py /test_source1/fs1/bad_filesystem")

        shelltest("zfs create -V 1M test_source1/fs1/ok_zvol")
        shelltest("dd if=/dev/urandom of=/dev/zvol/test_source1/fs1/ok_zvol count=1 bs=512k")

        shelltest("zfs create -V 1M test_source1/fs1/bad_zvol")
        shelltest("dd if=/dev/urandom of=/dev/zvol/test_source1/fs1/bad_zvol count=1 bs=512k")

        #create backup
        with mocktime("20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --no-progress --no-holds".split(" ")).run())

        #Do an ugly hack to create a fault in the bad filesystem
        #In zfs-autoverify it doenst matter that the snapshot isnt actually the same snapshot, so this hack works
        shelltest("zfs destroy test_target1/test_source1/fs1/bad_filesystem@test-20101111000000")
        shelltest("zfs mount test_target1/test_source1/fs1/bad_filesystem")
        shelltest("echo >> /test_target1/test_source1/fs1/bad_filesystem/test_verify.py")
        shelltest("zfs snapshot test_target1/test_source1/fs1/bad_filesystem@test-20101111000000")

        #do the same hack for the bad zvol
        shelltest("zfs destroy test_target1/test_source1/fs1/bad_zvol@test-20101111000000")
        shelltest("dd if=/dev/urandom of=/dev/zvol/test_target1/test_source1/fs1/bad_zvol count=1 bs=1")
        shelltest("zfs snapshot test_target1/test_source1/fs1/bad_zvol@test-20101111000000")


        # make sure we cant accidently compare current data
        shelltest("zfs mount test_target1/test_source1/fs1/ok_filesystem")
        shelltest("rm /test_source1/fs1/ok_filesystem/*")
        shelltest("rm /test_source1/fs1/bad_filesystem/*")
        shelltest("dd if=/dev/zero of=/dev/zvol/test_source1/fs1/ok_zvol count=1 bs=512k")



    def test_verify(self):


        with self.subTest("default --test"):
            self.assertFalse(ZfsAutoverify("test test_target1 --verbose --test".split(" ")).run())

        with self.subTest("rsync, remote source and target. (not supported, all 6 fail)"):
            self.assertEqual(6, ZfsAutoverify("test test_target1 --ssh-source=localhost --ssh-target=localhost --verbose --exclude-received --fs-compare=rsync".split(" ")).run())

        def runchecked(testname, command):
            with self.subTest(testname):
                with OutputIO() as buf:
                    result=None
                    with redirect_stderr(buf):
                        result=ZfsAutoverify(command.split(" ")).run()

                    print(buf.getvalue())
                    self.assertEqual(2,result)
                    self.assertRegex(buf.getvalue(), "bad_filesystem: FAILED:")
                    self.assertRegex(buf.getvalue(), "bad_zvol: FAILED:")

        runchecked("rsync, remote source", "test test_target1 --ssh-source=localhost --verbose --exclude-received --fs-compare=rsync")
        runchecked("rsync, remote target", "test test_target1 --ssh-target=localhost --verbose --exclude-received --fs-compare=rsync")
        runchecked("rsync, local", "test test_target1 --verbose --exclude-received --fs-compare=rsync")

        runchecked("tar, remote source and remote target",
                   "test test_target1 --ssh-source=localhost --ssh-target=localhost --verbose --exclude-received --fs-compare=find")
        runchecked("tar, remote source",
                   "test test_target1 --ssh-source=localhost --verbose --exclude-received --fs-compare=find")
        runchecked("tar, remote target",
                   "test test_target1 --ssh-target=localhost --verbose --exclude-received --fs-compare=find")
        runchecked("tar, local", "test test_target1 --verbose --exclude-received --fs-compare=find")

        with self.subTest("no common snapshot"):
            #destroy common snapshot, now 3 should fail
            shelltest("zfs destroy test_source1/fs1/ok_zvol@test-20101111000000")
            self.assertEqual(3, ZfsAutoverify("test test_target1 --verbose --exclude-received".split(" ")).run())

