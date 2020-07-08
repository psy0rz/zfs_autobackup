
from basetest import *


class TestZfsNode(unittest2.TestCase):

    def setUp(self):
        prepare_zpools()
        self.longMessage=True

    # generate a resumable state 
    def generate_resume(self):

        r=shelltest("zfs set compress=off test_source1 test_target1")

        #big change on source
        r=shelltest("dd if=/dev/zero of=/test_source1/fs1/data bs=250M count=1")

        #waste space on target
        r=shelltest("dd if=/dev/zero of=/test_target1/waste bs=250M count=1")

        #should fail and leave resume token (if supported)
        self.assertTrue(ZfsAutobackup("test test_target1 --verbose".split(" ")).run())

        #free up space
        r=shelltest("rm /test_target1/waste")
        #sync
        r=shelltest("zfs umount test_target1")
        r=shelltest("zfs mount test_target1")


    #resume initial backup
    def test_initial_resume(self):

        #inital backup, leaves resume token
        with patch('time.strftime', return_value="20101111000000"):
            self.generate_resume()

        #should resume and succeed
        with OutputIO() as buf:
            with redirect_stdout(buf):
                self.assertFalse(ZfsAutobackup("test test_target1 --verbose --debug".split(" ")).run())

            print(buf.getvalue())
            
            #did we really resume?
            if "0.6.5" in ZFS_USERSPACE:
                #abort this late, for beter coverage
                self.skipTest("Resume not supported in this ZFS userspace version")
            else:
                self.assertIn(": resuming", buf.getvalue())

        r=shelltest("zfs list -H -o name -r -t all test_target1")
        self.assertMultiLineEqual(r,"""
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
""")


    #resume incremental backup
    def test_incremental_resume(self):

        #initial backup
        with patch('time.strftime', return_value="20101111000000"):
            self.assertFalse(ZfsAutobackup("test test_target1 --verbose --allow-empty".split(" ")).run())

        #incremental backup leaves resume token
        with patch('time.strftime', return_value="20101111000001"):
            self.generate_resume()

        #should resume and succeed
        with OutputIO() as buf:
            with redirect_stdout(buf):
                self.assertFalse(ZfsAutobackup("test test_target1 --verbose --debug".split(" ")).run())

            print(buf.getvalue())
            
            #did we really resume?
            if "0.6.5" in ZFS_USERSPACE:
                #abort this late, for beter coverage
                self.skipTest("Resume not supported in this ZFS userspace version")
            else:
                self.assertIn(": resuming", buf.getvalue())

        r=shelltest("zfs list -H -o name -r -t all test_target1")
        self.assertMultiLineEqual(r,"""
test_target1
test_target1/test_source1
test_target1/test_source1/fs1
test_target1/test_source1/fs1@test-20101111000000
test_target1/test_source1/fs1@test-20101111000001
test_target1/test_source1/fs1/sub
test_target1/test_source1/fs1/sub@test-20101111000000
test_target1/test_source2
test_target1/test_source2/fs2
test_target1/test_source2/fs2/sub
test_target1/test_source2/fs2/sub@test-20101111000000
""")


    # # generate an invalid resume token, and verify if its aborted automaticly
    #FIXME: fails due to incorrectly created parent
    # def test_resumeabort(self):

    #     if "0.6.5" in ZFS_USERSPACE:
    #         self.skipTest("Resume not supported in this ZFS userspace version")

    #     #inital backup, leaves resume token
    #     with patch('time.strftime', return_value="20101111000000"):
    #         self.generate_resume()

    #     #remove corresponding source snapshot
    #     shelltest("zfs destroy test_source1/fs1@test-20101111000000")

    #     #try again, should abort old resume
    #     with patch('time.strftime', return_value="20101111000001"):
    #         self.assertFalse(ZfsAutobackup("test test_target1 --verbose --debug".split(" ")).run())
