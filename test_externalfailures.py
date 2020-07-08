
from basetest import *


class TestZfsNode(unittest2.TestCase):

    def setUp(self):
        prepare_zpools()
        self.longMessage=True


    def test_resume(self):

        if "0.6.5" in ZFS_USERSPACE:
            self.skipTest("Resume not supported in this ZFS userspace version")

        r=shelltest("zfs set compress=off test_source1 test_target1")

        #initial backup
        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --allow-empty".split(" ")).run())

        #big change on source
        r=shelltest("dd if=/dev/zero of=/test_source1/fs1/data bs=250M count=1")

        #waste space on target
        r=shelltest("dd if=/dev/zero of=/test_target1/waste bs=250M count=1")

        #should fail
        with patch('time.strftime', return_value="20101111000001"):
            self.assertTrue(ZfsAutobackup("test test_target1 --verbose --allow-empty".split(" ")).run())

        #free up space
        r=shelltest("rm /test_target1/waste")
        r=shelltest("zfs umount test_target1")


        #should resume and succeed
        with OutputIO() as buf:
            with redirect_stdout(buf):
                with patch('time.strftime', return_value="20101111000002"):
                    self.assertFalse(ZfsAutobackup("test test_target1 --verbose --allow-empty --debug".split(" ")).run())

            print(buf.getvalue())
            
            #did we really resume?
            self.assertIn(": resuming", buf.getvalue())



    # def test_resumeabort(self):

